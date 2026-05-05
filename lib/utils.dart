import 'dart:async';
import 'dart:io';

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

  Outcome._success(T value)
      : success = true,
        result = value,
        exception = null,
        stackTrace = null;

  Outcome._failure(Object error, StackTrace stackTrace)
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
/// If [signal] is aborted before [future] completes, throws [AbortException].
/// If [signal] is already aborted on entry, returns an immediately-failed
/// future.
Future<Outcome<T>> waitCancellable<T>(
  Future<T> future, [
  AbortSignal? signal,
]) {
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
        completer.complete(Outcome._success(value));
      }
    },
    onError: (Object error, StackTrace stackTrace) {
      registration?.unregister();
      if (!completer.isCompleted) {
        completer.complete(Outcome._failure(error, stackTrace));
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
    if (!completer.isCompleted) {
      completer.complete();
    }
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
Stream<T> streamCancellable<T>(Stream<T> stream, [AbortSignal? signal]) {
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

/// Connects a [Socket] to [host]:[port], with optional cancellation.
///
/// If [signal] is already aborted when called, throws [AbortException]
/// immediately. If it is aborted while the connection is in progress, the
/// attempt is cancelled and [AbortException] is thrown. A genuine connection
/// error (e.g. refused or unreachable) throws [SocketException] as normal.
Future<Socket> connectSocket(
  String host,
  int port, {
  AbortSignal? signal,
}) async {
  signal?.throwIfAborted();
  final task = await Socket.startConnect(host, port);
  final registration = signal?.register(task.cancel);
  try {
    return await task.socket;
  } on SocketException {
    if (signal?.aborted ?? false) {
      throw const AbortException();
    }
    rethrow;
  } finally {
    registration?.unregister();
  }
}

/// Connects a [SecureSocket] to [host]:[port], with optional cancellation.
///
/// If [signal] is already aborted when called, throws [AbortException]
/// immediately. If it is aborted while the connection is in progress, the
/// attempt is cancelled and [AbortException] is thrown. A genuine connection
/// error (e.g. refused or unreachable) throws [SocketException] as normal.
Future<SecureSocket> connectSecureSocket(
  String host,
  int port, {
  SecurityContext? context,
  bool Function(X509Certificate)? onBadCertificate,
  AbortSignal? signal,
}) async {
  signal?.throwIfAborted();
  final task = await SecureSocket.startConnect(
    host,
    port,
    context: context,
    onBadCertificate: onBadCertificate,
  );
  final registration = signal?.register(task.cancel);
  try {
    return await task.socket;
  } on SocketException {
    if (signal?.aborted ?? false) {
      throw const AbortException();
    }
    rethrow;
  } finally {
    registration?.unregister();
  }
}
