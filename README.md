# Dart cancel tokens code examples 

Part of what makes the [Dart programming language](https://dart.dev) great is its
excellent async support, but it is in dire need of a unified mechanism for cancellation.
This repository contains a proposal to add cancel tokens, modelled on [JavaScript's 
`AbortSignal`](https://developer.mozilla.org/en-US/docs/Web/API/AbortSignal) and [C#'s
`CancellationToken`](https://learn.microsoft.com/en-us/dotnet/standard/threading/cancellation-in-managed-threads).

It has a complete implementation of the cancellation token itself, a cancellation scope
modelled on [Trio's `CancelScope`](https://trio.readthedocs.io/en/stable/reference-core.html#cancellation-and-timeouts),
a few cancellable waiting primitives, cancellable TCP connect() functions, and a task
group class to allow structured concurrency.

**Discussion:** https://github.com/dart-lang/sdk/issues/63017

> [!NOTE]
> There are three variations of this proposal:
>
> 1. **`CancelController` based, with explicit token passing:** Modelled on JavaScript's `AbortController` and C#'s `CancellationTokenSource`, this just gives access to the token, with a single method `cancel()` to cancel it. Old revisions of this repo used this; `CancelController` is still present in `cancel_core.dart` for reference.
> 2. **`CancelScope` based, with explicit token passing:** Inspired by Trio's `CancelScope`, this is really just a glorified `try` / `on CancelException` block, but it's easier and safer to use than option 1 (especially when nesting scopes). This is the option presented below.
> 3. **`CancelScope` based, with implicit propagation:** More closely modelled on Trio's CancelScope; cancellation propagates automatically to all cancellation-aware async calls within a scope, so no tokens need to be passed around. The code for this is in the [`transparent-structured-concurrency` branch](https://github.com/arthur-tacca/dart-cancel-examples/tree/transparent-structured-concurrency). This is by far the best option but probably not feasible in Dart due to backwards compatibility issues (as explained in that branch's README).

## Contents

- [Example usage](#example-usage)
- [`lib/cancel_core.dart`](#libcancel_coredart)
- [`lib/cancellable.dart`](#libcancellabledart)
- [`lib/networking.dart`](#libnetworkingdart)
- [Task group usage](#task-group-usage)
- [`lib/task_group.dart`](#libtask_groupdart)
- [`bin/examples.dart`](#binexamplesdart)
- [`bin/main.dart`](#binmaindart)
- [Further development](#further-development)
- [License](#license)

## Example usage

To allow some code to be cancelled, you wrap it in a `CancelScope` and pass the scope's
`CancelToken` down to async functions in it. If you call the `cancel()` method then the
currently running function stops and throws a `CancelException`, which is caught by the
scope. Here's an example:

```dart
Future<Uint8List> readWithCancel() async {
  final scope = CancelScope();
  mightCallLater(() => scope.cancel());
  Uint8List? result;
  await scope.using((cancelToken) async {
    result = await readBytes('example.com', 8080, 100, cancelToken: cancelToken);
  });
  if (scope.cancelCaught) {
    throw MyException("Operation was interrupted");
  }
  return result!;
}
```

If the only reason to cancel is a deadline, you can use the convenience
wrapper `CancelScope.withTimeout()`, which allows returning a result
directly (and throws `TimeoutException` if timed out):

```dart
Future<Uint8List> readWithTimeout() async {
  return await CancelScope.withTimeout(
    timeout: Duration(seconds: 5),
    body: (cancelToken) async {
      return await readBytes('example.com', 8080, 100, cancelToken: cancelToken);
    },
  );
}
```

To write a function that supports being cancelled, typically you just have to
forward the token on to functions that you call, like this:

```dart
Future<Uint8List> readBytes(
  String host,
  int port,
  int byteCount, {
  CancelToken? cancelToken,
}) async {
  final socket = await connectSocket(host, port, cancelToken: cancelToken);
  try {
    final buffer = BytesBuilder(copy: false);
    await for (final chunk in streamCancellable(socket, cancelToken)) {
      buffer.add(chunk);
      if (buffer.length >= byteCount) break;
    }
    if (buffer.length < byteCount) {
      throw SocketException('Connection closed before $byteCount bytes received');
    }
    return buffer.takeBytes();
  } finally {
    socket.destroy();
  }
}
```

Ultimately, the leaf functions that perform low-level networking operations or
synchronisation primitives need to actually check when the cancel token fires.
For a good example of that, see the implementation of `connectSocket()` in
`lib/networking.dart`.

## `lib/cancel_core.dart`

The core cancellation types.

```dart
class CancelException implements Exception {}
```

Thrown when an operation is cancelled.

```dart
class StrayCancelError extends Error {}
```

Error thrown when a task threw `CancelException` even though its `CancelToken` was not cancelled.

This typically happens when a function correctly throws `CancelException` in response to its `CancelToken`, but the `Future` representing its result has been passed elsewhere and awaited in a different cancel scope. The best way to avoid this is structured concurrency: run tasks in task groups and communicate results by waiting for the task group to finish or use a channel (with the `oneWayChannel()` function).

```dart
class CancelTokenRegistration {
  void unregister();
}
class CancelToken {
  bool get cancelled;
  CancelTokenRegistration register(void Function() callback);
  void throwIfCancelled();
}
```
The cancel token itself. Obtained from a `CancelController`, or via the `body`
callback of `CancelScope.using()`. `register()` schedules a callback as a
microtask when the token is cancelled; call `unregister()` on its result to
remove its registration.

```dart
class CancelScope {
  CancelScope({
    CancelToken? parentCancelToken,
    Duration? timeout,
  });
  bool get cancelCaught;
  void cancel();
  void setTimeout(Duration timeout);
  Future<void> using(Future<void> Function(CancelToken) body);
  static Future<T> withTimeout<T>({
    required Future<T> Function(CancelToken) body,
    required Duration timeout,
    CancelToken? parentCancelToken,
  });
}
```

A cancel scope: creates and owns a `CancelToken`, allows cancelling it with `cancel()`,
and passes it to the body passed to `using()` (named after `using {...}`
blocks in C#). The `using()` method may be called only once per scope.
Use `cancelCaught` to determine afterwards if this scope was cancelled and
the body actually raised `CancelException` (analogous to Trio's
`CancelScope.cancelled_caught`).


```dart
class CancelController {
  CancelController({
    Duration? timeout,
    Iterable<CancelToken>? linkedCancelTokens,
  });
  CancelToken get cancelToken;
  void cancel();
}
```

Creates and owns a `CancelToken`. I recommend `CancelScope` as a more usable
alternative.

## `lib/cancellable.dart`

Cancellable wrappers around Dart's core async primitives.

```dart
Future<Outcome<T>> waitCancellable<T>(
  Future<T> future, [
  CancelToken? cancelToken,
]);
```
Waits for a [Future](https://api.dart.dev/dart-async/Future-class.html) to complete,
returning outcome as an `Outcome`. The wait can be interrupted with the cancel
token (but this does not cancel the function underlying the future).

```dart
class Outcome<T> {
  final bool success;
  final T? result;
  final Object? exception;
  final StackTrace? stackTrace;
  T get();
}
```
Captures the outcome of a `waitCancellable` call: either a success value or a
thrown exception with its original stack trace. `get()` returns the value or
re-throws with the original stack trace.

> [!NOTE]
> It might seem odd that this proxy object is returned rather than returning the result or throwing the exception directly. The reason for this is partly philosophical: if you're using `waitCancellable()` then that means the awaited operation is not really owned by this caller, so we shouldn't have the operation's exceptions raised directly. But it's mainly practical: it allows distinguishing a `CancelException` from the cancel token parameter versus one propagated from the operation.

```dart
Future<void> sleep(
  Duration duration, [
  CancelToken? cancelToken,
]);
```
Sleeps for the given duration, like 
[`Future.delayed`](https://api.dart.dev/dart-async/Future/Future.delayed.html)
(without the computation parameter) or
[`Timer`](https://api.flutter.dev/flutter/dart-async/Timer-class.html). Can be
interrupted by the cancel token.

> [!TIP]
> For a cancellable recurring timer, use
> [`Stream.periodic()`](https://api.dart.dev/dart-async/Stream/Stream.periodic.html)
> wrapped in `streamCancellable()`.

```dart
Stream<T> streamCancellable<T>(
  Stream<T> stream, [
  CancelToken? cancelToken,
]);
```
Wraps a [`Stream`](https://api.dart.dev/dart-async/Stream-class.html) so that a `CancelException` is injected and the source subscription cancelled when the token is cancelled.

This allows interrupting an `await for` while waiting for the next item in the
underlying stream. For example, if used on a `Stream.periodic()` stream with a
duration of 200ms and a cancel token that is cancelled after 500ms, the stream
will deliver an item after 200ms, then another after another 200ms, then throw
`CancelException` after another 100ms.

> [!WARNING]
> `streamCancellable()` uses
> [`StreamSubscription.cancel()`](https://api.dart.dev/dart-async/StreamSubscription/cancel.html),
> so it only works if the stream reacts promptly to cancellation. **It does not
> work for `async*` generators:** they don't react until their next item is
> ready, so cancellation is delayed as long as it takes to fetch the next item
> (possibly indefinitely). `streamCancellable()` can also lose an item that
> arrives just as it's cancelled. Again, this is worse for `async*` generators,
> which always fetch and discard their next item when interrupted by stream
> cancellation.
>
> For `async*` generators, slow-to-cancel streams, or anywhere you need
> guaranteed delivery, you should support cancel tokens in the stream itself
> instead. See [`generators.md`](generators.md) for details.

```dart
({Stream<T> stream, StreamSink<T> sink}) oneWayChannel<T>([
  CancelToken? cancelToken,
  bool drainOnCancel = true,
]);
```

Creates a multi-producer single-consumer (MPSC) queue, similar to those in many other async runtimes (e.g. [tokio mpsc](https://docs.rs/tokio/latest/tokio/sync/mpsc/) or [Trio memory channels](https://trio.readthedocs.io/en/stable/reference-core.html#using-channels-to-pass-values-between-tasks)). It's a bit like a one-way version of Dart's [`StreamChannel`](https://pub.dev/documentation/stream_channel/latest/stream_channel/). It's often useful for passing messages between different async tasks in a task group; each one makes its own channel and listens to it, and makes it available for other tasks to write to.

The implementation is simple: it's essentially just a [`StreamController`](https://api.dart.dev/dart-async/StreamController-class.html) with the `stream` and `sink` properties returned, and a bit of admin to wire up the cancellation to send a `CancelException` (before or after queued items, depending on `drainOnCancel`) and close the stream.

## `lib/networking.dart`

Cancellable TCP routines.

```dart
Future<Socket> connectSocket(
  String host,
  int port, {
  CancelToken? cancelToken,
});

Future<SecureSocket> connectSecureSocket(
  String host,
  int port, {
  SecurityContext? context,
  bool Function(X509Certificate)? onBadCertificate,
  CancelToken? cancelToken,
});
```
Cancellable TCP socket connections, built on
[`Socket.startConnect`](https://api.dart.dev/dart-io/Socket/startConnect.html) and
[`SecureSocket.startConnect`](https://api.dart.dev/dart-io/SecureSocket/startConnect.html).

The socket interface in Dart is stream based, which is problematic in some ways, but it means that most of the cancellation support comes almost for free.

* Listening for incoming connections to a socket server uses `Stream` so can be cancelled with `streamCancellable()`.
* Reading from a socket uses `Stream` so can be cancelled with `streamCancellable()`.
* Writing to a socket just pushes data to a queue so doesn't need cancellation support. You can drain the queue with `flush()` but that can just be abandoned using `waitCancellable()`.
* The confusingly named `destroy()` method to close the socket is synchronous (as methods to close asynchronous resources generally should be).

## Task group usage

The `TaskGroup` class encapsulates a safe wait for multiple concurrent tasks; if one throws an exception then the others are cancelled (with a `CancelToken`) and the task group continues to wait for them to complete. This technique is called structured concurrency, introduced in the excellent article [*Go statement considered harmful*](https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/).

You use a `TaskGroup` like this:

```dart
Future<Map<String, Uint8List>> remoteReads() async {
  final results = <String, Uint8List>{};
  TaskGroup taskGroup = TaskGroup();
  taskGroup.spawn((cancelToken) async {
    results['alpha'] = await readBytes('alpha.example.com', 8080, 100, cancelToken: cancelToken);
  });
  taskGroup.spawn((cancelToken) async {
    results['beta'] = await readBytes('beta.example.com', 8080, 100, cancelToken: cancelToken);
  });
  await taskGroup.waitComplete();
  return results;
}
```

The member method `TaskGroup.using()` is a helper that allows using a task group like a scope, like in most other languages that support structured concurrency. It spawns the body as a member task and waits for every task in the group, which is particularly useful for nesting multiple task groups correctly. Use it like this:

```dart
Future<Map<String, Uint8List>> remoteReads() async {
  final results = <String, Uint8List>{};
  final taskGroup = TaskGroup();
  await taskGroup.using((_) async {
    taskGroup.spawn((cancelToken) async {
      results['alpha'] = await readBytes('alpha.example.com', 8080, 100, cancelToken: cancelToken);
    });
    taskGroup.spawn((cancelToken) async {
      results['beta'] = await readBytes('beta.example.com', 8080, 100, cancelToken: cancelToken);
    });
  });
  return results;
}
```

The static method `TaskGroup.waitAll()` allows waiting for a fixed length list of tasks, similar to [`Future.wait()`](https://api.dart.dev/dart-async/Future/wait.html), which is useful when you don't need the full flexibility of `TaskGroup`. It uses a `TaskGroup` under the hood so it follows all the same rules when tasks throw an exception. This resolves the dilemma posed by the `eagerError` parameter on `Future.wait()`: if `false` then it could wait a long time to report an error, but if `true` then it gives no way to know when all tasks have finished. In contrast, `TaskGroup.waitAll()` waits until all tasks are done, but should return promptly after an error (if they handle cancellation suitably). Use it like this: 

```dart
Future<List<Uint8List>> remoteReads() async {
  return await TaskGroup.waitAll([
    (cancelToken) => readBytes('alpha.example.com', 8080, 100, cancelToken: cancelToken),
    (cancelToken) => readBytes('beta.example.com', 8080, 100, cancelToken: cancelToken),
  ]);
}
```

> [!NOTE]
> The file `bin/examples.dart` has two extra task group examples:
>
> * Happy eyeballs, the "hello world" of structured concurrency; starts multiple connection attempts staggered over time, picking the first to succeed
> * Nested task group server, showing how to nest groups; an outer one handles connections and an inner one handles listening ports

## `lib/task_group.dart`

Task group implementation. Composes a `CancelScope` (from `lib/cancel_core.dart`)
internally for cancellation lifetime.

```dart
class AggregateException implements Exception {
  final List<Object> exceptions;
  final List<StackTrace> stackTraces;
}
```

Thrown when one or more tasks fail; all the exceptions (except any `CancelException` instances) are collected into it, which ensures no failure is silently discarded.

```dart
class TaskGroup {
  TaskGroup({
    CancelToken? parentCancelToken,
  });
  bool get completed;
  CancelScope get scope;
  void spawn<T>(Future<T> Function(CancelToken) task);
  Future<Outcome<T>> spawnWithFuture<T>(Future<T> Function(CancelToken) task);
  Future<void> using(Future<void> Function(CancelToken) body);
  Future<void> waitComplete();
  static Future<List<T>> waitAll<T>(
    Iterable<Future<T> Function(CancelToken)> tasks, {
    CancelToken? parentCancelToken,
    Duration? timeout,
    void Function(T)? cleanUp,
  });
  static Future<T> waitAny<T>(
    Iterable<Future<T> Function(CancelToken)> tasks, {
    CancelToken? parentCancelToken,
    Duration? timeout,
    void Function(T)? cleanUp,
  });
}
```

Runs tasks and waits for them to complete. Use `spawn()` to start tasks and
`waitComplete()` to wait for them all to finish, or `using(body)` to spawn the
body as a member task and wait for everything in one call. So long as
`waitComplete()` has not yet returned, new tasks may continue to be spawned.
If any task throws an exception then all tasks are cancelled (to be more
precise: the token passed to all tasks is cancelled), but `waitComplete()`
still waits for them all to finish, and new tasks may even still be spawned.

`using()` may be called only once per task group; close over the task group
variable from the surrounding code to spawn additional tasks inside the body.

> [!NOTE]
> A task group can raise the following exceptions, listed in priority order:
>
> * `StrayCancelError` if a task throws `CancelException` despite the group's `CancelToken` not being cancelled at the end of the body (this is a programming error)
> * `AggregateException` if any task throws an exception other than `CancelException`
> * `CancelException` if at least one task observed the cancel (raised `CancelException`) and the parent token is cancelled
> * Otherwise, no exception is raised, even if `tg.scope.cancel()` has been called or `tg.scope.setTimeout()` expired (the task group consumes its own cancel). `tg.scope.cancelCaught` is true in that case. `TaskGroup.waitAll` and `TaskGroup.waitAny` lift the timeout case into a `TimeoutException` for the caller.
> 
> `TaskGroup.waitAll()` and `TaskGroup.waitAny()` raise `TimeoutException` if the timeout expires and there is no other exception raised (i.e. if `tg.scope.cancelCaught` is true).

## `bin/examples.dart`

Additional task group examples.

## `bin/main.dart`

A scratch file for manually testing and exercising the code in the other files.


## Further development

This code is already useful as it stands. But there are a few potential additions that could further improve it (besides tests and documentation!):

* **New base class for `CancelException`:** In this code, `CancelException` derives from `Exception` but that means it will be caught by any blanket `catch (Exception)` blocks. Ideally, Dart would introduce a new `BaseException` class, a shared base for `Exception` and `Error`, and `CancelException` could derive from that directly. As a bonus, if Dart later puts traceback information in the exception (where it belongs!), or adds exception chaining, then this gives a central place to put it. (This is inspired by Python's `BaseException`, which asyncio and Trio both use as the direct base for their cancellation exceptions.)
* **Support cancellable HTTP and web sockets:** Cancellation tokens finally give a natural way to specify when `HttpClient.getUrl()` and others should cancel (see https://github.com/dart-lang/sdk/issues/51267 – which actually suggests `AbortSignal`!). I haven't thought through the implications to the http API (e.g. should the cancel token passed to a request method be inherited by close() or should that take its own? And it looks like there needs to be a synchronous method to close a request that doesn't depend on whether it's got to response stage).  
* **Passing cancel tokens between isolates:** This is a bit more specialised, but would help with cancelling work delegated to other isolates. It would be a lot more intrusive than the other changes described here so it might be worth making it more general e.g. supporting a general [event object](https://en.wikipedia.org/wiki/Event_(computing)) passed between isolates (which is what a cancel token essentially is). This is safe so long as the destination can only read the value (i.e. for cancel tokens, you can only pass the token not the controller) and so long as it can't be unset (uncancelled). It should be possible to see an update without an event loop iteration, so that it's possible to interrupt a long running CPU operation if the user's code polls it occasionally.

## License 

Copyright 2026 Arthur Tacca

Dual licensed. You may use under the terms of either:

* 3 clause BSD license
* Boost Software License 1.0

I'm happy to be considered as one of the "Dart contributors" for attribution purposes, and happy to sign a CLA if needed.
