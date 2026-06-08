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

/// A dynamically-growable group of concurrent tasks sharing a cancellation
/// token. Tasks can be added at any point before [waitComplete] resolves.
///
/// Owns a [CancelScope] (exposed via [scope]) that handles parent-token
/// propagation, timeouts, and absorption of the group's own cancellation. The
/// scope forks a zone in which the group's token is the ambient
/// [currentCancelToken], so spawned tasks (and any code they call) pick it up
/// without needing the token threaded through as a parameter. Use the scope
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
  late final Zone _zone;
  int _pendingCount = 0;
  bool _sawCancel = false;
  bool _waitCompleteCalled = false;
  final Completer<void> _tasksDone = Completer<void>();
  late final Future<void> _waitFuture;
  final _exceptions = <Object>[];
  final _exceptionStackTraces = <StackTrace>[];

  /// Creates a task group. The parent token is the ambient [currentCancelToken]
  /// at construction time — so a [TaskGroup] created inside another group's
  /// body inherits cancellation from the enclosing group automatically. If
  /// [shield] is true, the group does **not** link to the ambient token —
  /// its tasks run uncancelled by the parent.
  ///
  /// To attach a timeout, call `tg.scope.setTimeout(...)` after
  /// construction. The group absorbs its own timeout — [waitComplete]
  /// returns normally with `scope.cancelCaught` set to true. Callers that
  /// want a thrown [TimeoutException] should use [waitAll] or [waitAny].
  TaskGroup({bool shield = false}) : _scope = CancelScope(shield: shield) {
    // Enter the scope eagerly so the body callback can capture the scope's
    // forked zone for synchronous use by spawn(). The body parks on
    // _tasksDone until the user signals "done spawning" by calling
    // waitComplete().
    _waitFuture = _scope.using(() async {
      _zone = Zone.current;
      await _tasksDone.future;
    });
    _finalizer.attach(this, StackTrace.current, detach: this);
  }

  /// Spawns [body] as a member task and waits for all tasks (including
  /// [body] itself) to complete.
  ///
  /// Exceptions thrown by [body] are collected alongside those of any other
  /// spawned tasks and reported as a single [AggregateException]. The body
  /// runs in the group's forked zone, so cancellable helpers inside it pick
  /// up the group's token as [currentCancelToken] without explicit threading.
  ///
  /// Calling `using` on a group that has already completed throws
  /// [StateError] — `spawn()` rejects tasks on a completed group.
  Future<void> using(Future<void> Function() body) {
    spawn(body);
    return waitComplete();
  }

  /// Runs [tasks] concurrently with shared cancellation, returning results in
  /// task order. If [timeout] is supplied and expires before all tasks
  /// complete, throws [TimeoutException] (with [cleanUp] applied to any
  /// partial results).
  static Future<List<T>> waitAll<T>(
    Iterable<Future<T> Function()> tasks, {
    bool shield = false,
    Duration? timeout,
    void Function(T)? cleanUp,
  }) async {
    final tg = TaskGroup(shield: shield);
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
      // No external canceller can reach this group, so the only thing that
      // could have absorbed a cancellation is the timeout firing.
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
  /// cancelled to cancel the others. If [timeout] is supplied and expires
  /// before any task succeeds, throws [TimeoutException].
  ///
  /// Throws [ArgumentError] if [tasks] is empty.
  ///
  /// Successful results are recorded in completion order. If more than one
  /// task produces a result and the group completes without exception,
  /// [cleanUp] is applied to every result except the first (the returned one).
  /// If the group throws, [cleanUp] is applied to every recorded result.
  static Future<T> waitAny<T>(
    Iterable<Future<T> Function()> tasks, {
    bool shield = false,
    Duration? timeout,
    void Function(T)? cleanUp,
  }) async {
    final taskList = tasks.toList();
    if (taskList.isEmpty) {
      throw ArgumentError.value(tasks, 'tasks', 'must not be empty');
    }
    final tg = TaskGroup(shield: shield);
    if (timeout != null) tg.scope.setTimeout(timeout);
    final results = <T>[];
    for (final task in taskList) {
      tg.spawn(() async {
        results.add(await task());
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
      // No task succeeded before the group's token was cancelled. The only
      // thing that can cancel it (with tasks present and no thrown
      // exception) is the timeout.
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
  /// The task runs in this group's forked zone, so [currentCancelToken] inside the
  /// task body resolves to the group's token. Cancellable helpers can be
  /// called without passing a token explicitly.
  ///
  /// If the group's token is already cancelled (due to a prior task failure or
  /// cancellation), the task is spawned anyway with the same cancelled token —
  /// it will see the cancel at its first token check.
  ///
  /// Throws [StateError] if the group has already completed.
  ///
  /// Use [spawnWithFuture] if you need to await the individual task's result.
  void spawn<T>(Future<T> Function() task) {
    spawnWithFuture(task);
  }

  /// Like [spawn], but returns the task's [Outcome] so the caller can await
  /// the individual result. The returned future never throws; exceptions are
  /// wrapped in the [Outcome] and also reported through [waitComplete].
  Future<Outcome<T>> spawnWithFuture<T>(Future<T> Function() task) {
    if (_tasksDone.isCompleted) {
      throw StateError('Cannot spawn a task on a completed TaskGroup');
    }
    _pendingCount++;

    Future<Outcome<T>> wrap() async {
      try {
        return Outcome.succeeded(await task());
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

    return _zone.run(wrap);
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
  /// Throws [AggregateException] if any task threw a non-[CancelException],
  /// or [CancelException] if at least one task observed the cancel and the
  /// parent token was cancelled. Task exceptions take priority over
  /// [CancelException]. If `scope.cancel()` was called or the timeout fired
  /// but the parent token was not cancelled, the group completes normally
  /// (its own cancel is consumed; `scope.cancelCaught` records that this
  /// happened). If a parent cancel or timeout fired but every task finished
  /// before noticing, the group also completes normally.
  Future<void> waitComplete() {
    if (!_waitCompleteCalled) {
      _waitCompleteCalled = true;
      _finalizer.detach(this);
      _checkTasksDone();
    }
    return _waitFuture;
  }
}
