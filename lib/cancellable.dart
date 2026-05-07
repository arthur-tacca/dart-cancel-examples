import 'dart:async';

import 'package:dart_cancel_examples/abort.dart';

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

/// Waits for [future], wrapping the outcome in an [Outcome].
///
/// Reads [currentSignal] at call time. If non-null, aborting it before
/// [future] completes throws [AbortException]; if it is already aborted on
/// entry, returns an immediately-failed future. If there is no ambient signal
/// the wait is uncancellable.
Future<Outcome<T>> waitCancellable<T>(Future<T> future) {
  final signal = currentSignal;
  if (signal != null && signal.aborted) {
    return Future.error(const AbortException());
  }

  final completer = Completer<Outcome<T>>();
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

/// Waits for [duration]. If [currentSignal] is non-null and aborts before the
/// timer fires, throws [AbortException] and cancels the timer.
///
/// `await sleep(Duration.zero)` (or any non-positive duration) is the
/// recommended application-code syntax for a cancellation point: it
/// throws if the ambient signal is already aborted, otherwise resolves
/// without scheduling a timer. Code that already knows about cancel
/// tokens — i.e. that registers its own abort callback for some other
/// reason — should use `currentSignal?.throwIfAborted()` directly.
Future<void> sleep(Duration duration) {
  final signal = currentSignal;
  if (signal != null && signal.aborted) {
    return Future.error(const AbortException());
  }
  if (duration <= Duration.zero) {
    return Future.value();
  }

  final completer = Completer<void>();
  AbortSignalRegistration? registration;

  final timer = Timer(duration, () {
    registration?.unregister();
    completer.complete();
  });

  registration = signal?.register(() {
    timer.cancel();
    registration!.unregister();
    completer.completeError(const AbortException());
  });

  return completer.future;
}

/// Wraps [stream] so that an [AbortException] error is injected and the source
/// subscription cancelled when [currentSignal] (captured at call time) aborts.
///
/// If there is no ambient signal the source stream is returned unwrapped. If
/// the ambient signal is already aborted on entry, returns an
/// immediately-errored stream.
///
/// Note: if the source stream buffers multiple events in a single event loop
/// turn, some may already be queued in the controller before abort fires,
/// and would be yielded before the [AbortException]. For well-behaved streams
/// that deliver at most one event per turn this is not an issue.
Stream<T> streamCancellable<T>(Stream<T> stream) {
  final signal = currentSignal;
  if (signal == null) {
    return stream;
  }
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
    if (!controller.isClosed) {
      controller.close();
    }
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
