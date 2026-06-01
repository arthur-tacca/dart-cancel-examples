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

/// Wraps [stream] so that the source subscription is cancelled when
/// [cancelToken] cancels, and a terminal error is then injected for the
/// consumer.
///
/// Cancellation is deferred so that the source is never cancelled while a loop
/// body is running. If the token cancels while the consumer is mid-iteration
/// (its subscription is paused), nothing happens immediately; the source is left
/// running and the cancellation is handled when the consumer next asks for a
/// value — at which point the source is cancelled and the terminal injected — or
/// when the consumer stops listening because its body threw or it broke out of
/// the `await for`, in which case the source is cancelled in `onCancel` and the
/// outcome propagated through the consumer's [StreamSubscription.cancel] future.
/// Because the body never runs concurrently with cancellation, a complex source
/// that hands out items backed by a resource it releases on cancel cannot have
/// that resource pulled out from under an in-flight loop body.
///
/// The source's cleanup outcome is captured: a [CancelException] if it completes
/// without error, or the cancel error if it throws, so a failure during
/// cancellation reaches the consumer rather than being lost. When the consumer
/// bails, that error is re-thrown from the [StreamSubscription.cancel] future so
/// it supersedes an in-flight loop-body exception, just as a raw `await for` over
/// the source would. Because the terminal waits for the source cleanup, a source
/// whose cancellation is slow or never completes will delay (or withhold) it.
/// When the source completes on its own, its subscription is already finished and
/// is not cancelled again.
///
/// A value whose delivery was already queued when the token cancelled is dropped
/// rather than handed to the consumer, so the loop body never sees a post-cancel
/// item (and, for an `async*` source, the value it had run one step ahead to
/// produce is suppressed).
///
/// If [cancelToken] is already cancelled on entry, the source is still briefly
/// subscribed and then cancelled so its cleanup runs, and the consumer receives
/// the [CancelException] without any buffered source events.
///
/// Because cancellation is deferred until the consumer next reads, a long-running
/// or infinite loop body keeps the source subscription (and any sockets or timers
/// it holds) alive until the body yields control. And, as with any stream, this
/// does NOT work well with `async*` generators: cancelling a subscription to a
/// generator only takes effect when it next reaches a `yield`, so a generator
/// parked on an `await` that does not itself observe the token is never cancelled
/// and the [CancelException] is never delivered. For generators, plumb the token
/// into the generator body itself (e.g. wrap its awaits with [waitCancellable] or
/// [sleep]) instead.
Stream<T> streamCancellable<T>(Stream<T> stream, [CancelToken? cancelToken]) {
  if (cancelToken == null) {
    return stream;
  }

  late final StreamController<T> controller;
  StreamSubscription<T>? subscription;
  CancelTokenRegistration? registration;
  // The source-stream cancellation, started exactly once (on resume or in
  // onCancel). cancelStream() swallows the stream's cancel() error so the
  // future always completes normally; the error is captured here and
  // re-surfaced through whichever channel the consumer ends up using.
  Future<void>? cancelStreamFuture;
  Object? cancelStreamError;
  StackTrace? cancelStreamStackTrace;

  // Cancel the source stream, capturing the outcome of its cleanup. Started at
  // most once; callers guard with `cancelStreamFuture ??= cancelStream()`.
  Future<void> cancelStream() async {
    final sub = subscription;
    subscription = null;
    try {
      await sub?.cancel();
    } catch (error, stackTrace) {
      cancelStreamError = error;
      cancelStreamStackTrace = stackTrace;
    }
  }

  // Consumer is (or is again) reading: once the stream cancellation has
  // finished, push the captured error (or a CancelException) and close.
  Future<void> addErrorAndClose() async {
    await (cancelStreamFuture ??= cancelStream());
    // The consumer may have bailed while we awaited the cancellation, in which
    // case onCancel already closed the controller; guard against adding then.
    if (!controller.isClosed) {
      if (cancelStreamError != null) {
        controller.addError(cancelStreamError!, cancelStreamStackTrace);
      } else {
        controller.addError(const CancelException());
      }
      controller.close();
    }
  }

  controller = StreamController<T>(
    onListen: () {
      // Register before listening to the source: if the token is already
      // cancelled this schedules the cancel callback ahead of any buffered
      // source events, so cancelling the source subscription suppresses them
      // and the consumer sees only the terminal error.
      registration = cancelToken.register(() {
        // Deferred cancellation: act only if the consumer is waiting for a
        // value. If it is mid-iteration (paused), do nothing now — leave the
        // stream running and handle the cancellation when the consumer next asks
        // for a value (onResume) or bails out of its loop (onCancel). The stream
        // is therefore never cancelled while a loop body is running, and the
        // delivery never races the consumer's own loop exit.
        if (!controller.isPaused) {
          addErrorAndClose();
        }
      });

      subscription = stream.listen(
        (value) {
          // Last-moment guard against the microtask race: a value's delivery can
          // already be queued when the token cancels. Once cancellation has
          // begun, drop it so the body never runs with a post-cancel value.
          if (cancelToken.cancelled || controller.isClosed) return;
          controller.add(value);
        },
        onError: controller.addError,
        onDone: () {
          // The source finished, so its subscription is already complete; there
          // is nothing to cancel. The source cannot deliver done after the
          // token/consumer close paths (both cancel this subscription), so the
          // controller is always still open here. Just release and close; the
          // registration is unregistered by onCancel, which the close triggers.
          subscription = null;
          controller.close();
        },
      );
    },
    onPause: () => subscription?.pause(),
    onResume: () {
      // An error held while a loop body ran is delivered here, instead of
      // resuming the source for another value.
      if (cancelToken.cancelled) {
        addErrorAndClose();
      } else {
        subscription?.resume();
      }
    },
    onCancel: () async {
      // The consumer stopped listening (broke out of its loop, or its body threw
      // and `await for` is now cancelling), or the source completed and the
      // controller closed. This is the single lifecycle endpoint for the token
      // registration: it always runs, so unregister here and nowhere else
      // (unregistering twice throws). Surface the stream cancellation error
      // through this cancel() future so it supersedes any in-flight loop-body
      // exception, exactly as a raw `await for` over the source would. If the
      // error was already pushed through the stream (controller closed), the
      // consumer is receiving it that way, so add nothing here.
      registration?.unregister();
      registration = null;
      await (cancelStreamFuture ??= cancelStream());
      if (controller.isClosed) return;
      controller.close();
      if (cancelStreamError != null) {
        Error.throwWithStackTrace(cancelStreamError!, cancelStreamStackTrace!);
      }
    },
  );

  return controller.stream;
}

/// Creates a single-consumer message queue backed by a [StreamController].
///
/// Returns the consumer-facing [stream] and the producer-facing [sink]. Any
/// number of producers may push to [sink] without awaiting, while a single
/// consumer drains [stream]; events pushed before the consumer subscribes are
/// buffered.
///
/// When the consumer stops listening — by cancelling its subscription or
/// breaking out of an `await for` — the controller is closed, so producers are
/// notified: [StreamSink.done] completes and any later [StreamSink.add] throws a
/// [StateError]. The notification is asynchronous, so a producer racing the
/// shutdown must still guard `add`.
///
/// If [cancelToken] is supplied, cancelling it interrupts the consumer and
/// closes the controller. [drainOnCancel] selects what the consumer sees: when
/// true (the default) the queued items are delivered first and the
/// [CancelException] follows; when false the [CancelException] is raised ahead of
/// any items still queued, dropping them. If [cancelToken] is already cancelled
/// when [oneWayChannel] is called, [stream] errors on first listen and the
/// controller is closed immediately.
///
/// When [drainOnCancel] is true the controller is closed as soon as the token
/// cancels, so producers are barred immediately (as above). When it is false the
/// interrupt is driven by [streamCancellable], whose cancellation is deferred
/// while the consumer is mid-iteration: the controller is then closed only once
/// the consumer next reads or stops listening, so a producer may still `add`
/// (silently dropped) in the window between the token cancelling and the consumer
/// next being read.
({Stream<T> stream, StreamSink<T> sink}) oneWayChannel<T>([
  CancelToken? cancelToken,
  bool drainOnCancel = true,
]) {
  final controller = StreamController<T>();
  CancelTokenRegistration? registration;

  controller.onCancel = () {
    // Fire-and-forget: returning close()'s future here would deadlock, because
    // cancel() awaits onCancel while close() awaits delivery to the listener
    // that is itself mid-cancel.
    registration?.unregister();
    registration = null;
    if (!controller.isClosed) {
      controller.close();
    }
  };

  if (cancelToken == null) {
    return (stream: controller.stream, sink: controller.sink);
  }

  if (drainOnCancel) {
    // Single controller: the CancelException is queued after any buffered
    // items, so the consumer drains them first. close() still bars producers
    // immediately. (The controller is the source, so there is no upstream
    // subscription to tear down.)
    void cancel() {
      // Unregister so the entry does not linger in a long-lived token's list,
      // keeping this closure (and the controller) reachable. Safe when run from
      // the token callback (the entry is still linked) and a no-op in the
      // already-cancelled branch below (registration is still null).
      registration?.unregister();
      registration = null;
      if (!controller.isClosed) {
        controller.addError(const CancelException());
        controller.close();
      }
    }

    if (cancelToken.cancelled) {
      cancel();
    } else {
      registration = cancelToken.register(cancel);
    }
    return (stream: controller.stream, sink: controller.sink);
  } else {
    // Interrupt ahead of queued items: streamCancellable injects the
    // CancelException ahead of items still held upstream by backpressure
    // (dropping them), and cancelling its subscription fires onCancel -> close,
    // which bars producers. The drop is backpressure-dependent: a consumer that
    // never pauses leaves nothing buffered upstream to drop. Note that
    // streamCancellable defers cancellation while the consumer is mid-iteration,
    // so the close (and thus the barring of producers) does not happen until the
    // consumer next reads or stops listening; a producer racing that window may
    // still add an item, which is then silently dropped.
    return (
      stream: streamCancellable(controller.stream, cancelToken),
      sink: controller.sink,
    );
  }
}
