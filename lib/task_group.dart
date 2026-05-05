import 'dart:async';

import 'package:dart_cancel_examples/abort.dart';

/// Thrown by [TaskGroup.waitAll] and [TaskGroup.waitComplete] when one or more
/// tasks fail with a non-[AbortException].
class AggregateException implements Exception {
  /// The non-[AbortException] exceptions thrown by tasks, in receipt order.
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
/// signal. Tasks can be added at any point before [waitComplete] completes.
class TaskGroup {
  static final _finalizer = Finalizer<StackTrace>((creationTrace) {
    throw StateError(
      'TaskGroup GC\'d without waitComplete() being called\n'
      'TaskGroup created at:\n$creationTrace',
    );
  });

  final AbortController _controller;
  final AbortSignal? _parentSignal;
  final bool _raiseOnTimeout;
  final Duration? _timeout;
  int _pendingCount = 0;
  bool _spuriousAbort = false;
  bool _didTimeout = false;
  Completer<void>? _completer;
  Timer? _timer;
  AbortSignalRegistration? _parentRegistration;
  final _exceptions = <Object>[];
  final _exceptionStackTraces = <StackTrace>[];
  final _creationTrace = StackTrace.current;

  /// Creates a task group. If [parentSignal] is supplied, aborting it also
  /// aborts this group. If [timeout] is supplied, the group is aborted after
  /// that duration; if [raiseOnTimeout] is true (the default) a
  /// [TimeoutException] is thrown by [waitComplete], otherwise it completes
  /// normally with [didTimeout] set to true.
  TaskGroup({
    AbortSignal? parentSignal,
    Duration? timeout,
    bool raiseOnTimeout = true,
  })  : _controller = AbortController(),
        _parentSignal = parentSignal,
        _raiseOnTimeout = raiseOnTimeout,
        _timeout = timeout {
    _finalizer.attach(this, _creationTrace, detach: this);
    if (parentSignal != null) {
      if (parentSignal.aborted) {
        _controller.abort();
      } else {
        _parentRegistration = parentSignal.register(_controller.abort);
      }
    }
    if (timeout != null) {
      _timer = Timer(timeout, () {
        _didTimeout = true;
        _controller.abort();
      });
    }
  }

  /// Creates a task group, passes it to [body] as a spawned task, then waits
  /// for all tasks (including [body] itself) to complete.
  ///
  /// Exceptions thrown by [body] are collected alongside those of any other
  /// spawned tasks and reported as a single [AggregateException].
  static Future<void> using(
    Future<void> Function(TaskGroup) body, {
    AbortSignal? parentSignal,
    Duration? timeout,
    bool raiseOnTimeout = true,
  }) {
    final tg = TaskGroup(
      parentSignal: parentSignal,
      timeout: timeout,
      raiseOnTimeout: raiseOnTimeout,
    );
    tg.spawn((_) => body(tg));
    return tg.waitComplete();
  }

  /// Runs [tasks] concurrently with shared cancellation, returning results in
  /// task order.
  static Future<List<T>> waitAll<T>(
    Iterable<Future<T> Function(AbortSignal)> tasks, {
    AbortSignal? parentSignal,
    Duration? timeout,
    void Function(T)? cleanUp,
  }) async {
    final tg = TaskGroup(
      parentSignal: parentSignal,
      timeout: timeout,
    );
    final futures = [for (final task in tasks) tg.spawnWithFuture(task)..ignore()];

    final results = <T>[];
    for (final f in futures) {
      try {
        results.add(await f);
      } catch (_) {}
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

  /// The shared cancellation signal for all tasks in this group.
  AbortSignal get signal => _controller.signal;

  /// Whether all tasks have completed and [waitComplete] has resolved.
  bool get completed => _completer?.isCompleted ?? false;

  /// Whether the group's [timeout] (if any) fired before completion.
  bool get didTimeout => _didTimeout;

  /// Aborts the group's own cancellation token, cancelling all running tasks.
  ///
  /// Throws [StateError] if the group has already completed.
  void abort() {
    if (completed) {
      throw StateError('Cannot abort a completed TaskGroup');
    }
    _controller.abort();
  }

  void _checkCompleted() {
    if (_pendingCount != 0 || _completer == null) {
      return;
    }
    _timer?.cancel();
    _timer = null;
    _parentRegistration?.unregister();
    _parentRegistration = null;
    if (_spuriousAbort) {
      _completer!.completeError(StateError(
        'A task in this TaskGroup threw AbortException without its '
        'AbortSignal being aborted',
      ));
    } else if (_exceptions.isNotEmpty) {
      _completer!.completeError(
        AggregateException(_exceptions, _exceptionStackTraces),
      );
    } else if (_parentSignal?.aborted ?? false) {
      _completer!.completeError(const AbortException());
    } else if (didTimeout && _raiseOnTimeout) {
      _completer!.completeError(TimeoutException('TaskGroup timed out', _timeout));
    } else {
      _completer!.complete();
    }
  }

  /// Spawns [task] as a concurrent member of this group.
  ///
  /// If the group's signal is already aborted (due to a prior task failure or
  /// cancellation), the task is spawned anyway with the same aborted signal —
  /// it will see the abort at its first signal check.
  ///
  /// Throws [StateError] if the group has already completed.
  ///
  /// Use [spawnWithFuture] if you need to await the individual task's result.
  void spawn<T>(Future<T> Function(AbortSignal) task) {
    spawnWithFuture(task).ignore();
  }

  /// Like [spawn], but returns the task's future so the caller can await it.
  ///
  /// Errors are also reported through [waitComplete]; callers that do not
  /// await the returned future should use [spawn] instead to avoid unhandled
  /// future warnings.
  Future<T> spawnWithFuture<T>(Future<T> Function(AbortSignal) task) {
    if (_completer?.isCompleted ?? false) {
      throw StateError('Cannot spawn a task on a completed TaskGroup');
    }
    _pendingCount++;
    final future = task(signal);

    Future<T> wrap() async {
      try {
        return await future;
      } catch (error, stackTrace) {
        if (error is AbortException) {
          if (!signal.aborted) {
            _spuriousAbort = true;
          }
        } else {
          _exceptions.add(error);
          _exceptionStackTraces.add(stackTrace);
        }
        _controller.abort();
        rethrow;
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
  /// Throws [AggregateException] if any task threw a non-[AbortException],
  /// [AbortException] if the parent signal was aborted, or [TimeoutException]
  /// if a timeout was set, it fired, and [raiseOnTimeout] is true. Task
  /// exceptions take priority over [AbortException], which takes priority over
  /// [TimeoutException]. If [abort] was called but the parent signal was not
  /// aborted, the group completes normally (its own abort is consumed).
  Future<void> waitComplete() {
    if (_completer == null) {
      _finalizer.detach(this);
      _completer = Completer<void>();
      _checkCompleted();
    }
    return _completer!.future;
  }
}
