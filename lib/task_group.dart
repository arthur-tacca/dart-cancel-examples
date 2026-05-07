import 'dart:async';

import 'package:dart_cancel_examples/abort.dart';
import 'package:dart_cancel_examples/cancellable.dart';

// Note: there is deliberately no top-level `spawn()`, `spawnWithFuture()` or
// `currentTaskGroup` accessor. Exposing the enclosing group would let an
// async function spawn tasks that outlive its own call — breaking the
// structured-concurrency invariant that a function's lifetime bounds the
// lifetime of the work it starts. Tasks must be spawned through an explicit
// [TaskGroup] reference passed in by the caller, which makes the bound
// visible at the call site.

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
///
/// The group's signal is exposed as the ambient [currentSignal] inside every
/// task it runs (and inside the body passed to [using]), so cancellable
/// helpers like [sleep] and [connectSocket] pick it up automatically without
/// needing the signal threaded through as a parameter.
class TaskGroup {
  static final _finalizer = Finalizer<StackTrace>((creationTrace) {
    throw StateError(
      'TaskGroup GC\'d without waitComplete() being called\n'
      'TaskGroup created at:\n$creationTrace',
    );
  });

  final AbortController _controller;
  final AbortSignal? _parentSignal;
  final bool _shield;
  final bool _raiseOnTimeout;
  Duration? _timeout;
  int _pendingCount = 0;
  bool _spuriousAbort = false;
  bool _didTimeout = false;
  Completer<void>? _completer;
  Timer? _timer;
  AbortSignalRegistration? _parentRegistration;
  final _exceptions = <Object>[];
  final _exceptionStackTraces = <StackTrace>[];
  final _creationTrace = StackTrace.current;
  late final Zone _zone;

  /// Creates a task group. The group's parent signal is always [currentSignal]
  /// — so a [TaskGroup] created inside another group's body inherits
  /// cancellation from the enclosing group automatically. There is no way to
  /// pass a parent signal explicitly: external cancellation sources are wired
  /// in by registering a callback that calls [abort] on the resulting group.
  ///
  /// If [shield] is true, the group does **not** link to the enclosing
  /// group's signal — its tasks run uncancelled by the parent. This is
  /// analogous to Trio's `CancelScope.shield` and lets you run cleanup or
  /// otherwise-required work from inside an already-cancelled group. The
  /// group's own [abort] and [timeout] still apply. Defaults to false.
  ///
  /// If [timeout] is supplied, the group is aborted after that duration; if
  /// [raiseOnTimeout] is true (the default) a [TimeoutException] is thrown by
  /// [waitComplete], otherwise it completes normally with [didTimeout] set to
  /// true.
  TaskGroup({
    bool shield = false,
    Duration? timeout,
    bool raiseOnTimeout = true,
  })  : _controller = AbortController(),
        _parentSignal = shield ? null : currentSignal,
        _shield = shield,
        _raiseOnTimeout = raiseOnTimeout,
        _timeout = timeout {
    _finalizer.attach(this, _creationTrace, detach: this);
    final ps = _parentSignal;
    if (ps != null) {
      if (ps.aborted) {
        _controller.abort();
      } else {
        _parentRegistration = ps.register(_controller.abort);
      }
    }
    if (timeout != null) {
      _timer = Timer(timeout, () {
        _didTimeout = true;
        _controller.abort();
      });
    }
    _zone = forkZoneWithSignal(_controller.signal);
  }

  /// Creates a task group, passes it to [body] as a spawned task, then waits
  /// for all tasks (including [body] itself) to complete.
  ///
  /// Exceptions thrown by [body] are collected alongside those of any other
  /// spawned tasks and reported as a single [AggregateException].
  static Future<void> using({
    required Future<void> Function(TaskGroup) body,
    bool shield = false,
    Duration? timeout,
    bool raiseOnTimeout = true,
  }) {
    final tg = TaskGroup(
      shield: shield,
      timeout: timeout,
      raiseOnTimeout: raiseOnTimeout,
    );
    tg.spawn(() => body(tg));
    return tg.waitComplete();
  }

  /// Runs [tasks] concurrently with shared cancellation, returning results in
  /// task order.
  static Future<List<T>> waitAll<T>(
    Iterable<Future<T> Function()> tasks, {
    bool shield = false,
    Duration? timeout,
    void Function(T)? cleanUp,
  }) async {
    final tg = TaskGroup(shield: shield, timeout: timeout);
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

  /// The shared cancellation signal for all tasks in this group. Exposed as
  /// the ambient [currentSignal] inside the group's zone.
  AbortSignal get signal => _controller.signal;

  /// Whether all tasks have completed and [waitComplete] has resolved.
  bool get completed => _completer?.isCompleted ?? false;

  /// Whether the group's [timeout] (if any) fired before completion.
  bool get didTimeout => _didTimeout;

  /// Whether this group was created with `shield: true`, in which case it
  /// did not link to the enclosing group's cancellation signal.
  bool get shield => _shield;

  /// Aborts the group's own cancellation token, cancelling all running tasks.
  void abort() {
    _controller.abort();
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
      _controller.abort();
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
  /// The task runs in this group's forked zone, so [currentSignal] inside the
  /// task body resolves to [signal]. Cancellable helpers can be called without
  /// passing a signal explicitly.
  ///
  /// If the group's signal is already aborted (due to a prior task failure or
  /// cancellation), the task is spawned anyway with the same aborted signal —
  /// it will see the abort at its first signal check.
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
    if (_completer?.isCompleted ?? false) {
      throw StateError('Cannot spawn a task on a completed TaskGroup');
    }
    _pendingCount++;

    Future<Outcome<T>> wrap() async {
      try {
        return Outcome.succeeded(await task());
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
        return Outcome.failed(error, stackTrace);
      } finally {
        _pendingCount--;
        _checkCompleted();
      }
    }

    return _zone.run(wrap);
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
