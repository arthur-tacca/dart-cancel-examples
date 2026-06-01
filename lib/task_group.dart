import 'dart:async';

import 'package:dart_cancel_examples/cancel_core.dart';
import 'package:dart_cancel_examples/cancellable.dart';

/// Thrown by [TaskGroup.waitComplete] when one or more tasks failed with a
/// non-[CancelException]. [CancelException] instances are never stored here;
/// they're tracked separately and reported as a bare [CancelException] when
/// there are no other failures.
class AggregateException implements Exception {
  /// The non-[CancelException] exceptions thrown by tasks, in receipt order.
  final List<Object> exceptions;

  /// The stack traces corresponding to each entry in [exceptions].
  final List<StackTrace> stackTraces;

  AggregateException(this.exceptions, this.stackTraces);

  @override
  String toString() =>
      'AggregateException: ${exceptions.length} task(s) failed:\n'
      '${exceptions.map((e) => '  $e').join('\n')}';
}

/// A dynamically-growable group of concurrent tasks sharing a cancel token.
/// Tasks can be added at any point before [waitComplete] resolves.
///
/// Owns a [CancelScope] (exposed via [scope]) that handles parent-token
/// propagation and absorption of the group's own cancellation. Use the scope
/// for cancellation operations: `tg.scope.cancel()`, `tg.scope.setTimeout(...)`,
/// `tg.scope.cancelCaught`.
class TaskGroup {
  static final _finalizer = Finalizer<StackTrace>((creationTrace) {
    throw StateError(
      'TaskGroup GC\'d without waitComplete() being called\n'
      'TaskGroup created at:\n$creationTrace',
    );
  });

  final CancelScope _scope;
  late final CancelToken _cancelToken;
  int _pendingCount = 0;
  bool _sawCancel = false;
  bool _waitCompleteCalled = false;
  final Completer<void> _tasksDone = Completer<void>();
  late final Future<void> _waitFuture;
  final _exceptions = <Object>[];
  final _exceptionStackTraces = <StackTrace>[];

  /// Creates a task group. If [parentCancelToken] is supplied, cancelling it also
  /// cancels this group. For deadline-based cancel, call
  /// `tg.scope.setTimeout(...)` after construction.
  TaskGroup({
    CancelToken? parentCancelToken,
  }) : _scope = CancelScope(parentCancelToken: parentCancelToken) {
    // Enter the scope eagerly so the body callback can capture its token for
    // synchronous use by spawn(). The body parks on _tasksDone until the user
    // signals "done spawning" by calling waitComplete().
    _waitFuture = _scope.using((cancelToken) async {
      _cancelToken = cancelToken;
      await _tasksDone.future;
    });
    _finalizer.attach(this, StackTrace.current, detach: this);
  }

  /// Spawns [body] as a member task and waits for all tasks (including
  /// [body] itself) to complete.
  ///
  /// Exceptions thrown by [body] are collected alongside those of any other
  /// spawned tasks and reported as a single [AggregateException]. The body
  /// receives this group's [CancelToken] as a parameter, matching the spawn
  /// callback signature.
  ///
  /// May be called only once per task group; subsequent calls (or calls
  /// after [waitComplete]) throw [StateError].
  Future<void> using(Future<void> Function(CancelToken) body) {
    if (_waitCompleteCalled) {
      throw StateError(
        'TaskGroup.using() called after waitComplete() (or a previous using())',
      );
    }
    spawn(body);
    return waitComplete();
  }

  /// Runs [tasks] concurrently with shared cancellation, returning results in
  /// task order. If [timeout] is supplied and expires (and at least one task
  /// observes the cancel), [TimeoutException] is thrown.
  static Future<List<T>> waitAll<T>(
    Iterable<Future<T> Function(CancelToken)> tasks, {
    CancelToken? parentCancelToken,
    Duration? timeout,
    void Function(T)? cleanUp,
  }) async {
    final tg = TaskGroup(parentCancelToken: parentCancelToken);
    if (timeout != null) tg.scope.setTimeout(timeout);
    final futures = [for (final task in tasks) tg.spawnWithFuture(task)];

    final results = <T>[];
    for (final f in futures) {
      final outcome = await f;
      if (outcome.success) results.add(outcome.get());
    }

    try {
      await tg.waitComplete();
    } catch (_) {
      if (cleanUp != null) {
        for (final v in results) {
          cleanUp(v);
        }
      }
      rethrow;
    }

    if (tg.scope.cancelCaught) {
      // The only cancel source here is the timeout — waitAll doesn't expose
      // the scope and parent cancel would have thrown out of waitComplete.
      if (cleanUp != null) {
        for (final v in results) {
          cleanUp(v);
        }
      }
      throw TimeoutException('TaskGroup.waitAll timed out', timeout);
    }

    return results;
  }

  /// Runs [tasks] concurrently with shared cancellation, returning the first
  /// successful result. When any task finishes successfully, the group is
  /// cancelled, cancelling the others.
  ///
  /// Throws [ArgumentError] if [tasks] is empty, or [TimeoutException] if
  /// [timeout] is supplied and expires before any task succeeds.
  ///
  /// Successful results are recorded in completion order. If more than one
  /// task produces a result and the group completes without exception,
  /// [cleanUp] is applied to every result except the first (the returned one).
  /// If the group throws, [cleanUp] is applied to every recorded result.
  static Future<T> waitAny<T>(
    Iterable<Future<T> Function(CancelToken)> tasks, {
    CancelToken? parentCancelToken,
    Duration? timeout,
    void Function(T)? cleanUp,
  }) async {
    final taskList = tasks.toList();
    if (taskList.isEmpty) {
      throw ArgumentError.value(tasks, 'tasks', 'must not be empty');
    }
    final tg = TaskGroup(parentCancelToken: parentCancelToken);
    if (timeout != null) tg.scope.setTimeout(timeout);
    final results = <T>[];
    for (final task in taskList) {
      tg.spawn((cancelToken) async {
        results.add(await task(cancelToken));
        tg.scope.cancel();
      });
    }

    try {
      await tg.waitComplete();
    } catch (_) {
      if (cleanUp != null) {
        for (final v in results) {
          cleanUp(v);
        }
      }
      rethrow;
    }

    if (results.isEmpty) {
      // The empty-input case threw above, so an empty results list here means
      // the timeout fired before any task could record a result.
      throw TimeoutException('TaskGroup.waitAny timed out', timeout);
    }
    if (cleanUp != null) {
      for (var i = 1; i < results.length; i++) {
        cleanUp(results[i]);
      }
    }
    return results.first;
  }

  /// The [CancelScope] for this group's cancellation, timeout, and parent
  /// token handling.
  CancelScope get scope => _scope;

  /// Whether all tasks have completed.
  bool get completed => _tasksDone.isCompleted;

  /// Spawns [task] as a concurrent member of this group.
  ///
  /// If the group's token is already cancelled (due to a prior task failure or
  /// cancellation), the task is spawned anyway with the same cancelled token —
  /// it will see the cancel at its first token check.
  ///
  /// Throws [StateError] if the group has already completed.
  ///
  /// Use [spawnWithFuture] if you need to await the individual task's result.
  void spawn<T>(Future<T> Function(CancelToken) task) {
    spawnWithFuture(task);
  }

  /// Like [spawn], but returns the task's [Outcome] so the caller can await
  /// the individual result. The returned future never throws; exceptions are
  /// wrapped in the [Outcome] and also reported through [waitComplete].
  Future<Outcome<T>> spawnWithFuture<T>(Future<T> Function(CancelToken) task) {
    if (_tasksDone.isCompleted) {
      throw StateError('Cannot spawn a task on a completed TaskGroup');
    }
    _pendingCount++;
    final future = task(_cancelToken);

    Future<Outcome<T>> wrap() async {
      try {
        return Outcome.succeeded(await future);
      } catch (error, stackTrace) {
        if (error is CancelException) {
          _sawCancel = true;
        } else if (error is AggregateException) {
          // Flatten so that there are no nested AggregateException instances
          _exceptions.addAll(error.exceptions);
          _exceptionStackTraces.addAll(error.stackTraces);
        } else {
          _exceptions.add(error);
          _exceptionStackTraces.add(stackTrace);
        }
        _scope.cancel();
        return Outcome.failed(error, stackTrace);
      } finally {
        _pendingCount--;
        _checkTasksDone();
      }
    }

    return wrap();
  }

  void _checkTasksDone() {
    if (_pendingCount != 0 || !_waitCompleteCalled || _tasksDone.isCompleted) {
      return;
    }
    if (_exceptions.isNotEmpty) {
      _tasksDone.completeError(
        AggregateException(_exceptions, _exceptionStackTraces),
      );
    } else if (_sawCancel) {
      _tasksDone.completeError(const CancelException());
    } else {
      _tasksDone.complete();
    }
  }

  /// Waits until all spawned tasks have completed, then resolves.
  ///
  /// The group cannot complete until this is called — it provides the trigger
  /// that allows an initially-empty group to settle. Repeated calls return the
  /// same future.
  ///
  /// Throws [AggregateException] if any task threw a non-[CancelException], or
  /// [CancelException] if at least one task observed the cancel and the parent
  /// token was cancelled. Task exceptions take priority over [CancelException].
  /// If `scope.cancel()` was called (e.g. via `scope.setTimeout(...)` expiring)
  /// but the parent token was not cancelled, the group completes normally (its
  /// own cancel is consumed) and [scope].`cancelCaught` is true. If a parent
  /// cancel fired but every task finished before noticing, the group also
  /// completes normally.
  Future<void> waitComplete() {
    if (!_waitCompleteCalled) {
      _waitCompleteCalled = true;
      _finalizer.detach(this);
      _checkTasksDone();
    }
    return _waitFuture;
  }
}
