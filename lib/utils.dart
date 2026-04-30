import 'dart:async';
import 'dart:io';

import 'package:dart_cancel_examples/abort.dart';

/// The outcome of a completed operation, capturing either a success value or
/// a thrown exception.
class Completed<T> {
  /// Whether the operation succeeded normally rather than throwing.
  final bool success;

  /// The result value, if [success] is true.
  final T? result;

  /// The thrown exception, if [success] is false.
  final Object? exception;

  /// The stack trace from the throw site, if [success] is false.
  final StackTrace? stackTrace;

  Completed._success(T value)
      : success = true,
        result = value,
        exception = null,
        stackTrace = null;

  Completed._failure(Object error, StackTrace stackTrace)
      : success = false,
        result = null,
        exception = error,
        stackTrace = stackTrace;

  /// Returns [result] if [success] is true, otherwise re-throws [exception]
  /// with its original stack trace.
  T get() => success
      ? result as T
      : Error.throwWithStackTrace(exception!, stackTrace!);
}

/// Waits for [future], wrapping the outcome in a [Completed].
///
/// If [signal] is aborted before [future] completes, throws [AbortException].
/// If [signal] is already aborted on entry, returns an immediately-failed
/// future.
Future<Completed<T>> waitCancellable<T>(
  Future<T> future, [
  AbortSignal? signal,
]) {
  if (signal != null && signal.aborted) {
    return Future.error(const AbortException());
  }

  final completer = Completer<Completed<T>>();
  AbortSignalRegistration? registration;

  if (signal != null) {
    registration = signal.register(() {
      registration!.unregister();
      registration = null;
      completer.completeError(const AbortException());
    });
  }

  future.then(
    (value) {
      // Unregister before completing so that if abort was also scheduled in
      // the same turn, its entry.list check will suppress it.
      registration?.unregister();
      if (!completer.isCompleted) {
        completer.complete(Completed._success(value));
      }
    },
    onError: (Object error, StackTrace stackTrace) {
      registration?.unregister();
      if (!completer.isCompleted) {
        completer.complete(Completed._failure(error, stackTrace));
      }
    },
  );

  return completer.future;
}

/// Waits for [duration], throwing [AbortException] if [signal] is aborted
/// first. Also cancels the underlying timer when aborted.
Future<void> sleep(Duration duration, [AbortSignal? signal]) {
  if (signal != null && signal.aborted) {
    return Future.error(const AbortException());
  }

  final completer = Completer<void>();
  AbortSignalRegistration? registration;

  final timer = Timer(duration, () {
    registration?.unregister();
    // Guard needed: timer may have already been queued when abort called
    // timer.cancel(), leaving both the timer callback and the abort microtask
    // in flight. Abort wins by completing first; timer callback defers.
    if (!completer.isCompleted) completer.complete();
  });

  registration = signal?.register(() {
    timer.cancel();
    registration!.unregister();
    completer.completeError(const AbortException());
  });

  return completer.future;
}

/// Wraps [stream] so that an [AbortException] error is injected and the source
/// subscription cancelled when [signal] aborts.
///
/// If [signal] is already aborted on entry, returns an immediately-errored
/// stream.
///
/// Note: if the source stream buffers multiple events in a single event loop
/// turn, some may already be queued in the controller before abort fires,
/// and would be yielded before the [AbortException]. For well-behaved streams
/// that deliver at most one event per turn this is not an issue.
Stream<T> streamCancellable<T>(Stream<T> stream, AbortSignal signal) {
  if (signal.aborted) {
    return Stream.error(const AbortException());
  }

  final controller = StreamController<T>();
  StreamSubscription<T>? subscription;
  AbortSignalRegistration? registration;

  void cleanup() {
    registration?.unregister();
    registration = null;
    subscription?.cancel();
    subscription = null;
    if (!controller.isClosed) controller.close();
  }

  controller.onListen = () {
    registration = signal.register(() {
      if (!controller.isClosed) {
        controller.addError(const AbortException());
        cleanup();
      }
    });

    subscription = stream.listen(
      controller.add,
      onError: controller.addError,
      onDone: cleanup,
    );
  };

  controller.onPause = () => subscription?.pause();
  controller.onResume = () => subscription?.resume();
  controller.onCancel = cleanup;

  return controller.stream;
}

/// Thrown by [waitAll] when one or more tasks fail with a non-[AbortException].
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

/// Runs all [tasks] concurrently, each receiving a shared [AbortSignal].
///
/// Returns a list of results in task order if all succeed.
///
/// If any task throws a non-[AbortException], the rest are cancelled via the
/// shared signal and [waitAll] waits for all to finish before throwing an
/// [AggregateException] containing every non-abort exception.
///
/// If [signal] is aborted and no task throws a non-abort exception, throws
/// [AbortException].
///
/// [cleanUp] is called on the result of every task that succeeded when the
/// overall operation is failing, to allow resources to be released.
///
/// Throws [StateError] if a task throws [AbortException] without its signal
/// being aborted — this is a programming error in the task.
Future<List<T>> waitAll<T>(
  Iterable<Future<T> Function(AbortSignal)> tasks, {
  AbortSignal? signal,
  void Function(T)? cleanUp,
}) async {
  final internalController = AbortController();
  final taskSignal = signal != null
      ? AbortSignal.any([signal, internalController.signal])
      : internalController.signal;

  // Wraps one task: captures its result or exception directly into shared
  // state, aborting sibling tasks and running cleanUp as early as possible.
  // Returns the value on success, null on failure (failures are tracked via
  // hasFailure, not the null return, so T=null? works correctly).
  // Non-abort exceptions are appended to exceptions/exceptionStackTraces in
  // arrival order. Sets spuriousAbort if a task throws AbortException when
  // the signal isn't aborted — that's a programming error in the task.
  var spuriousAbort = false;
  var hasFailure = false;
  final succeeded = <T>[];
  final exceptions = <Object>[];
  final exceptionStackTraces = <StackTrace>[];
  Future<T?> wrap(Future<T> Function(AbortSignal) task) async {
    try {
      final value = await task(taskSignal);
      if (hasFailure) {
        cleanUp?.call(value);
      } else if (cleanUp != null) {
        succeeded.add(value);
      }
      return value;
    } catch (error, stackTrace) {
      if (error is AbortException && !taskSignal.aborted) spuriousAbort = true;
      internalController.abort();
      if (!hasFailure) {
        hasFailure = true;
        for (final v in succeeded) cleanUp?.call(v);
        succeeded.clear();
      }
      if (error is! AbortException) {
        exceptions.add(error);
        exceptionStackTraces.add(stackTrace);
      }
      return null;
    }
  }

  // Start all tasks concurrently, then collect results in task order.
  final futures = tasks.map(wrap).toList();
  final results = [for (final f in futures) await f];

  // A spurious AbortException is a programmer error regardless of what other
  // tasks did, so check for it before inspecting exceptions.
  if (spuriousAbort) {
    throw StateError(
      'A task passed to waitAll threw AbortException without its '
      'AbortSignal being aborted',
    );
  }

  if (exceptions.isNotEmpty) {
    throw AggregateException(exceptions, exceptionStackTraces);
  }

  // If there are remaining failures they must all be AbortExceptions caused
  // by the external signal (spuriousAbort is false, so the signal was aborted).
  if (hasFailure) {
    throw const AbortException();
  }

  return results.map((v) => v as T).toList();
}

/// Like [waitAll] but for tasks with no return value. Has no [cleanUp]
/// parameter. Otherwise behaves identically.
Future<void> waitAllSimple(
  Iterable<Future<void> Function(AbortSignal)> tasks, {
  AbortSignal? signal,
}) async {
  final internalController = AbortController();
  final taskSignal = signal != null
      ? AbortSignal.any([signal, internalController.signal])
      : internalController.signal;

  var spuriousAbort = false;
  var hasAbortFailure = false;
  final exceptions = <Object>[];
  final exceptionStackTraces = <StackTrace>[];

  Future<void> wrap(Future<void> Function(AbortSignal) task) async {
    try {
      await task(taskSignal);
    } catch (error, stackTrace) {
      if (error is AbortException && !taskSignal.aborted) spuriousAbort = true;
      internalController.abort();
      if (error is AbortException) {
        hasAbortFailure = true;
      } else {
        exceptions.add(error);
        exceptionStackTraces.add(stackTrace);
      }
    }
  }

  final futures = tasks.map(wrap).toList();
  for (final f in futures) await f;

  if (spuriousAbort) {
    throw StateError(
      'A task passed to waitAllSimple threw AbortException without its '
      'AbortSignal being aborted',
    );
  }

  if (exceptions.isNotEmpty) {
    throw AggregateException(exceptions, exceptionStackTraces);
  }

  if (hasAbortFailure) {
    throw const AbortException();
  }
}

/// Like [waitAll] but returns [Future<void>] and populates [results] (if
/// provided) with a [Completed<T>] for each task that either succeeded or
/// threw a non-[AbortException] — keyed by the same string used in [tasks].
/// Tasks that threw [AbortException] are omitted from [results]. The map is
/// populated before any exception is thrown, so callers can inspect partial
/// results in a catch block. Has no [cleanUp] parameter since the caller has
/// full information via [results].
Future<void> waitAllAlt<T>(
  Map<String, Future<T> Function(AbortSignal)> tasks, {
  AbortSignal? signal,
  Map<String, Completed<T>>? results,
}) async {
  final internalController = AbortController();
  final taskSignal = signal != null
      ? AbortSignal.any([signal, internalController.signal])
      : internalController.signal;

  // Wraps one task: catches any exception, aborting the shared signal so
  // sibling tasks are cancelled promptly. Writes into results directly,
  // omitting AbortException failures. Non-abort exceptions are appended to
  // exceptions/exceptionStackTraces in arrival order. Sets spuriousAbort if a
  // task throws AbortException when the signal isn't aborted; that's a
  // programming error in the task.
  var spuriousAbort = false;
  var hasAbortFailure = false;
  final exceptions = <Object>[];
  final exceptionStackTraces = <StackTrace>[];
  Future<void> wrap(
    MapEntry<String, Future<T> Function(AbortSignal)> entry,
  ) async {
    try {
      final value = await entry.value(taskSignal);
      results?[entry.key] = Completed._success(value);
    } catch (error, stackTrace) {
      if (error is AbortException && !taskSignal.aborted) spuriousAbort = true;
      internalController.abort();
      if (error is AbortException) {
        hasAbortFailure = true;
        return;
      }
      exceptions.add(error);
      exceptionStackTraces.add(stackTrace);
      results?[entry.key] = Completed._failure(error, stackTrace);
    }
  }

  // Start all tasks concurrently, then wait for all to finish.
  final futures = tasks.entries.map(wrap).toList();
  for (final f in futures) await f;

  // A spurious AbortException is a programmer error regardless of what other
  // tasks did, so check for it before inspecting exceptions.
  if (spuriousAbort) {
    throw StateError(
      'A task passed to waitAllAlt threw AbortException without its '
      'AbortSignal being aborted',
    );
  }

  if (exceptions.isNotEmpty) {
    throw AggregateException(exceptions, exceptionStackTraces);
  }

  // Remaining failures are AbortExceptions caused by the external signal.
  if (hasAbortFailure) {
    throw const AbortException();
  }
}

/// Connects a [Socket] to [host]:[port], with optional cancellation.
///
/// If [abortSignal] is already aborted when called, throws [AbortException]
/// immediately. If it is aborted while the connection is in progress, the
/// attempt is cancelled and [AbortException] is thrown. A genuine connection
/// error (e.g. refused or unreachable) throws [SocketException] as normal.
Future<Socket> connectSocket(
  String host,
  int port, {
  AbortSignal? abortSignal,
}) async {
  abortSignal?.throwIfAborted();
  final task = await Socket.startConnect(host, port);
  final abortRegistration = abortSignal?.register(() => task.cancel());
  try {
    return await task.socket;
  } on SocketException {
    if (abortSignal?.aborted ?? false) {
      throw const AbortException();
    }
    rethrow;
  } finally {
    abortRegistration?.unregister();
  }
}
