import 'dart:async';

import 'package:dart_cancel_examples/cancel_core.dart';
import 'package:dart_cancel_examples/cancellable.dart';

/// Thrown by [TaskGroup.waitAll] and [TaskGroup.waitComplete] when one or more
/// tasks fail with a non-[CancelException].
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
/// token. Tasks can be added at any point before [waitComplete] completes.
class TaskGroup {
  static final _finalizer = Finalizer<StackTrace>((creationTrace) {
    throw StateError(
      'TaskGroup GC\'d without waitComplete() being called\n'
      'TaskGroup created at:\n$creationTrace',
    );
  });

  final CancelController _controller;
  final CancelToken? _parentCancelToken;
  final bool _raiseOnTimeout;
  Duration? _timeout;
  int _pendingCount = 0;
  bool _spuriousCancellation = false;
  bool _sawLegitimateCancellation = false;
  bool _didTimeout = false;
  bool _cancelCaught = false;
  Completer<void>? _completer;
  Timer? _timer;
  CancelTokenRegistration? _parentRegistration;
  final _exceptions = <Object>[];
  final _exceptionStackTraces = <StackTrace>[];
  final _creationTrace = StackTrace.current;

  /// Creates a task group. If [parentCancelToken] is supplied, cancelling it also
  /// cancels this group. If [timeout] is supplied, the group is cancelled after
  /// that duration; if [raiseOnTimeout] is true (the default) a
  /// [TimeoutException] is thrown by [waitComplete], otherwise it completes
  /// normally with [didTimeout] set to true.
  TaskGroup({
    CancelToken? parentCancelToken,
    Duration? timeout,
    bool raiseOnTimeout = true,
  })  : _controller = CancelController(),
        _parentCancelToken = parentCancelToken,
        _raiseOnTimeout = raiseOnTimeout,
        _timeout = timeout {
    _finalizer.attach(this, _creationTrace, detach: this);
    if (parentCancelToken != null) {
      if (parentCancelToken.cancelled) {
        _controller.cancel();
      } else {
        _parentRegistration = parentCancelToken.register(_controller.cancel);
      }
    }
    if (timeout != null) {
      _timer = Timer(timeout, () {
        _didTimeout = true;
        _controller.cancel();
      });
    }
  }

  /// Creates a task group, passes it to [body] as a spawned task, then waits
  /// for all tasks (including [body] itself) to complete.
  ///
  /// Exceptions thrown by [body] are collected alongside those of any other
  /// spawned tasks and reported as a single [AggregateException].
  static Future<void> using({
    required Future<void> Function(TaskGroup) body,
    CancelToken? parentCancelToken,
    Duration? timeout,
    bool raiseOnTimeout = true,
  }) {
    final tg = TaskGroup(
      parentCancelToken: parentCancelToken,
      timeout: timeout,
      raiseOnTimeout: raiseOnTimeout,
    );
    tg.spawn((_) => body(tg));
    return tg.waitComplete();
  }

  /// Runs [tasks] concurrently with shared cancellation, returning results in
  /// task order.
  static Future<List<T>> waitAll<T>(
    Iterable<Future<T> Function(CancelToken)> tasks, {
    CancelToken? parentCancelToken,
    Duration? timeout,
    void Function(T)? cleanUp,
  }) async {
    final tg = TaskGroup(
      parentCancelToken: parentCancelToken,
      timeout: timeout,
    );
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

    return results;
  }

  /// Runs [tasks] concurrently with shared cancellation, returning the first
  /// successful result. When any task finishes successfully, the group is
  /// cancelled to cancel the others.
  ///
  /// Throws [ArgumentError] if [tasks] is empty.
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
    final tg = TaskGroup(
      parentCancelToken: parentCancelToken,
      timeout: timeout,
    );
    final results = <T>[];
    for (final task in tasks) {
      tg.spawn((cancelToken) async {
        results.add(await task(cancelToken));
        tg.cancel();
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
      throw ArgumentError.value(tasks, 'tasks', 'must not be empty');
    }
    if (cleanUp != null) {
      for (var i = 1; i < results.length; i++) {
        cleanUp(results[i]);
      }
    }
    return results.first;
  }

  /// The shared cancellation token for all tasks in this group.
  CancelToken get cancelToken => _controller.cancelToken;

  /// Whether all tasks have completed and [waitComplete] has resolved.
  bool get completed => _completer?.isCompleted ?? false;

  /// Whether the group's [timeout] (if any) fired before completion.
  bool get didTimeout => _didTimeout;

  /// Whether this group absorbed its own cancellation, analogous to Trio's
  /// `CancelScope.cancelled_caught`. True if at least one task threw
  /// [CancelException] due to this group's own token being cancelled (not a
  /// parent token), and no spurious cancellations or other task failures
  /// occurred.
  bool get cancelCaught => _cancelCaught;

  /// Cancels the group's own cancellation token, cancelling all running tasks.
  void cancel() {
    _controller.cancel();
  }

  /// Replaces any existing timeout with a new one.
  ///
  /// Does nothing if the group has already completed.
  void setTimeout(Duration timeout) {
    _timeout = timeout;
    if (completed) return;
    _timer?.cancel();
    _timer = Timer(timeout, () {
      _didTimeout = true;
      _controller.cancel();
    });
  }

  void _checkCompleted() {
    if (_pendingCount != 0 || _completer == null) {
      return;
    }
    _timer?.cancel();
    _timer = null;
    _parentRegistration?.unregister();
    _parentRegistration = null;
    if (_spuriousCancellation) {
      _completer!.completeError(StateError(
        'A task in this TaskGroup threw CancelException without its '
        'CancelToken being cancelled',
      ));
    } else if (_exceptions.isNotEmpty) {
      _completer!.completeError(
        AggregateException(_exceptions, _exceptionStackTraces),
      );
    } else if (_sawLegitimateCancellation) {
      if (_parentCancelToken?.cancelled ?? false) {
        _completer!.completeError(const CancelException());
      } else {
        _cancelCaught = true;
        if (didTimeout && _raiseOnTimeout) {
          _completer!.completeError(TimeoutException('TaskGroup timed out', _timeout));
        } else {
          _completer!.complete();
        }
      }
    } else {
      _completer!.complete();
    }
  }

  /// Spawns [task] as a concurrent member of this group.
  ///
  /// If the group's token is already cancelled (due to a prior task failure or
  /// cancellation), the task is spawned anyway with the same cancelled token —
  /// it will see the cancellation at its first token check.
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
    if (_completer?.isCompleted ?? false) {
      throw StateError('Cannot spawn a task on a completed TaskGroup');
    }
    _pendingCount++;
    final future = task(cancelToken);

    Future<Outcome<T>> wrap() async {
      try {
        return Outcome.succeeded(await future);
      } catch (error, stackTrace) {
        if (error is CancelException) {
          if (!cancelToken.cancelled) {
            _spuriousCancellation = true;
          } else {
            _sawLegitimateCancellation = true;
          }
        } else if (error is AggregateException) {
          // Flatten so that there are no nested AggregateException instances
          _exceptions.addAll(error.exceptions);
          _exceptionStackTraces.addAll(error.stackTraces);
        } else {
          _exceptions.add(error);
          _exceptionStackTraces.add(stackTrace);
        }
        _controller.cancel();
        return Outcome.failed(error, stackTrace);
      } finally {
        _pendingCount--;
        _checkCompleted();
      }
    }

    return wrap();
  }

  /// Waits until all spawned tasks have completed, then resolves.
  ///
  /// The group cannot complete until this is called — it provides the trigger
  /// that allows an initially-empty group to settle. Repeated calls return the
  /// same future.
  ///
  /// Throws [AggregateException] if any task threw a non-[CancelException],
  /// [CancelException] if the parent token was cancelled, or [TimeoutException]
  /// if a timeout was set, it fired, and [raiseOnTimeout] is true. Task
  /// exceptions take priority over [CancelException], which takes priority over
  /// [TimeoutException]. If [cancel] was called but the parent token was not
  /// cancelled, the group completes normally (its own cancellation is consumed).
  Future<void> waitComplete() {
    if (_completer == null) {
      _finalizer.detach(this);
      _completer = Completer<void>();
      _checkCompleted();
    }
    return _completer!.future;
  }
}
