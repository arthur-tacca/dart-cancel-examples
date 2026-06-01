import 'dart:async';

import 'package:dart_cancel_examples/cancel_core.dart';

/// The outcome of a completed operation, capturing either a success value or
/// a thrown exception.
class Outcome<T> {
  /// Whether the operation succeeded normally rather than throwing.
  final bool success;

  /// The result value, if [success] is true.
  final T? result;

  /// The thrown exception, if [success] is false.
  final Object? exception;

  /// The stack trace from the throw site, if [success] is false.
  final StackTrace? stackTrace;

  Outcome.succeeded(T value)
      : success = true,
        result = value,
        exception = null,
        stackTrace = null;

  Outcome.failed(Object error, StackTrace stackTrace)
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

/// Waits for [future], wrapping the outcome in a [Outcome].
///
/// If [cancelToken] is cancelled before [future] completes, throws [CancelException].
/// If [cancelToken] is already cancelled on entry, returns an immediately-failed
/// future.
Future<Outcome<T>> waitCancellable<T>(
  Future<T> future, [
  CancelToken? cancelToken,
]) {
  if (cancelToken != null && cancelToken.cancelled) {
    return Future.error(const CancelException());
  }

  final completer = Completer<Outcome<T>>();
  CancelTokenRegistration? registration;

  if (cancelToken != null) {
    registration = cancelToken.register(() {
      registration!.unregister();
      registration = null;
      completer.completeError(const CancelException());
    });
  }

  future.then(
    (value) {
      // Unregister before completing so that if cancel was also scheduled in
      // the same turn, its entry.list check will suppress it.
      registration?.unregister();
      if (!completer.isCompleted) {
        completer.complete(Outcome.succeeded(value));
      }
    },
    onError: (Object error, StackTrace stackTrace) {
      registration?.unregister();
      if (!completer.isCompleted) {
        completer.complete(Outcome.failed(error, stackTrace));
      }
    },
  );

  return completer.future;
}

/// Waits for [duration], throwing [CancelException] if [cancelToken] is cancelled
/// first. Also cancels the underlying timer when cancelled.
Future<void> sleep(Duration duration, [CancelToken? cancelToken]) {
  if (cancelToken != null && cancelToken.cancelled) {
    return Future.error(const CancelException());
  }

  final completer = Completer<void>();
  CancelTokenRegistration? registration;

  final timer = Timer(duration, () {
    registration?.unregister();
    completer.complete();
  });

  registration = cancelToken?.register(() {
    timer.cancel();
    registration!.unregister();
    completer.completeError(const CancelException());
  });

  return completer.future;
}

/// Wraps [stream] so that a [CancelException] error is injected and the source
/// subscription cancelled when [cancelToken] cancels.
///
/// If [cancelToken] is already cancelled on entry, returns an immediately-errored
/// stream.
///
/// Note: if the source stream buffers multiple events in a single event loop
/// turn, some may already be queued in the controller before cancel fires,
/// and would be yielded before the [CancelException]. For well-behaved streams
/// that deliver at most one event per turn this is not an issue.
Stream<T> streamCancellable<T>(Stream<T> stream, [CancelToken? cancelToken]) {
  if (cancelToken == null) {
    return stream;
  }
  if (cancelToken.cancelled) {
    return Stream.error(const CancelException());
  }

  final controller = StreamController<T>();
  StreamSubscription<T>? subscription;
  CancelTokenRegistration? registration;

  void cleanup() {
    registration?.unregister();
    registration = null;
    subscription?.cancel();
    subscription = null;
    if (!controller.isClosed) {
      controller.close();
    }
  }

  controller.onListen = () {
    registration = cancelToken.register(() {
      if (!controller.isClosed) {
        controller.addError(const CancelException());
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
