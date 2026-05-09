# Dart cancel tokens code examples 

This repository contains example Dart code to accompany a proposal to add
cancellation tokens to the [Dart programming language](https://dart.dev).
The design is modelled on JavaScript's `AbortController`/`AbortSignal` and C#'s
`CancellationTokenSource`/`CancellationToken`. 

It has a complete implementation
of the cancellation token itself, a few cancellable waiting primitives,
a task group class to allow structured concurrency, and cancellable TCP connect() functions.

- **Proposal:** https://gist.github.com/arthur-tacca/accbd333a6378619936e34d184b0d152
- **Discussion:** https://github.com/dart-lang/sdk/issues/63017
- **Code examples:** https://github.com/arthur-tacca/dart-cancel-examples
- **Alternative approach (transparent structured concurrency):**
  https://github.com/arthur-tacca/dart-cancel-examples/tree/transparent-structured-concurrency

## Contents

- [Example usage](#example-usage)
- [`lib/abort.dart`](#libabootdart)
- [`lib/cancellable.dart`](#libcancellabledart)
- [`lib/networking.dart`](#libnetworkingdart)
- [Task group usage](#task-group-usage)
- [`lib/task_group.dart`](#libtask_groupdart)
- [`bin/examples.dart`](#binexamplesdart)
- [`bin/main.dart`](#binmaindart)
- [Further development](#further-development)
- [License](#license)

## Example usage

A typical usage to create a token and pass it to a function that supports cancellation
would look like this:

```dart
Future<Uint8List> remoteRead() async {
  final controller = AbortController();
  mightCallLater(() => controller.abort());
  try {
    return await readBytes('example.com', 8080, 100, signal: controller.signal);
  } on AbortException {
    // It's rude to let an internal AbortException leak out of a function
    throw MyException("Operation was interrupted");
  }
}
```

To write a function that supports being cancelled, typically you just have to
forward the token on to functions that you call, like this:

```dart
Future<Uint8List> readBytes(
  String host,
  int port,
  int byteCount, {
  AbortSignal? signal,
}) async {
  final socket = await connectSocket(host, port, signal: signal);
  try {
    final buffer = BytesBuilder(copy: false);
    await for (final chunk in streamCancellable(socket, signal)) {
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

## `lib/abort.dart`

The core cancellation types.

```dart
class AbortController {
  AbortController({
    Duration? timeout,
    Iterable<AbortSignal>? linkedSignals,
  });
  AbortSignal get signal;
  void abort();
}
```
Creates and owns an `AbortSignal`. The signal is aborted when `abort()` is called, when the `timeout` expires, or when any of the linked signals is aborted. (The linked signals parameter is most often used with a list of length 1, representing a parent operation's signal.)

> [!NOTE]
> The resources associated with the timeout (a `Timer`) and linked signals
> (an `AbortSignalRegistration` closure capturing locals) will be cleaned up 
> when the signal is aborted (for any reason). Applications that could use a
> large number of signals should explicitly call `abort()` on them when they
> are no longer needed to avoid leaking resources.
>
> The design originally followed JavaScript's model of having `AbortSignal.timeout()` and `AbortSignal.any()` instead of these parameters on `AbortController`. That design is a bit neater, but gives no interface to clean up resources.
> 
> This is not a concern if using task groups. Those automatically clean up 
> resources (timer and signal registration) when they complete.

```dart
class AbortSignal {
  bool get aborted;
  void throwIfAborted();
  AbortSignalRegistration register(void Function() callback);
}
```
The cancel token itself. Obtained from an `AbortController`.
`register()` schedules a callback as a microtask when the signal aborts.

```dart
class AbortException implements Exception {}
```

Thrown when an operation is cancelled.

```dart
class AbortSignalRegistration {
  void unregister();
}
```
Represents a callback registered with an `AbortSignal`; call `unregister()` to
remove it.

## `lib/cancellable.dart`

Cancellable wrappers around Dart's core async primitives.

```dart
Future<Outcome<T>> waitCancellable<T>(
  Future<T> future, [
  AbortSignal? signal,
]);
```
Waits for a [Future](https://api.dart.dev/dart-async/Future-class.html) to complete,
returning outcome as an `Outcome`. The wait can be interrupted with the abort
signal (but this does not cancel the function underlying the future).

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
> It might seem odd that this proxy object is returned rather than returning the result or throwing the exception directly. The reason for this is partly philosophical: if you're using `waitCancellable()` then that means the awaited operation is not really owned by this caller, so we shouldn't have the operation's exceptions raised directly. But it's mainly practical: it allows distinguishing an `AbortException` from the signal parameter versus one propagated from the operation.

```dart
Future<void> sleep(
  Duration duration, [
  AbortSignal? signal,
]);
```
Sleeps for the given duration, like 
[`Future.delayed`](https://api.dart.dev/dart-async/Future/Future.delayed.html)
(without the computation parameter) or
[`Timer`](https://api.flutter.dev/flutter/dart-async/Timer-class.html). Can be
interrupted by the abort signal.

> [!TIP]
> For a cancellable recurring timer, use
> [`Stream.periodic()`](https://api.dart.dev/dart-async/Stream/Stream.periodic.html)
> wrapped in `streamCancellable()`.

```dart
Stream<T> streamCancellable<T>(
  Stream<T> stream, [
  AbortSignal? signal,
]);
```
Wraps a [Stream](https://api.dart.dev/dart-async/Stream-class.html) so that an
`AbortException` is injected and the source subscription cancelled when the
signal aborts.

## `lib/networking.dart`

Cancellable TCP routines.

```dart
Future<Socket> connectSocket(
  String host,
  int port, {
  AbortSignal? signal,
});

Future<SecureSocket> connectSecureSocket(
  String host,
  int port, {
  SecurityContext? context,
  bool Function(X509Certificate)? onBadCertificate,
  AbortSignal? signal,
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

The `TaskGroup` class encapsulates a safe wait for multiple concurrent tasks; if one throws an exception then the others are cancelled (with an `AbortSignal`) and the task group continues to wait for them to complete. This technique is called structured concurrency, introduced in the excellent article [*Go statement considered harmful*](https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/).

You use a `TaskGroup` like this:

```dart
Future<Map<String, Uint8List>> remoteReads() async {
  final results = <String, Uint8List>{};
  TaskGroup taskGroup = TaskGroup();
  taskGroup.spawn((signal) async {
    results['alpha'] = await readBytes('alpha.example.com', 8080, 100, signal: signal);
  });
  taskGroup.spawn((signal) async {
    results['beta'] = await readBytes('beta.example.com', 8080, 100, signal: signal);
  });
  await taskGroup.waitComplete();
  return results;
}
```

The static method `TaskGroup.using()` (named after `using {...}` blocks in C#) is a helper that allows using a task group like a scope, like in most other languages that support structured concurrency. It's particularly useful for nesting multiple task groups correctly. Use it like this: 

```dart
Future<Map<String, Uint8List>> remoteReads() async {
  final results = <String, Uint8List>{};
  await TaskGroup.using(body: (taskGroup) async {
    taskGroup.spawn((signal) async {
      results['alpha'] = await readBytes('alpha.example.com', 8080, 100, signal: signal);
    });
    taskGroup.spawn((signal) async {
      results['beta'] = await readBytes('beta.example.com', 8080, 100, signal: signal);
    });
  });
  return results;
}
```

The static method `TaskGroup.waitAll()` allows waiting for a fixed length list of tasks, similar to [`Future.wait()`](https://api.dart.dev/dart-async/Future/wait.html), which is useful when you don't need the full flexibility of `TaskGroup`. It uses a `TaskGroup` under the hood so it follows all the same rules when tasks throw an exception. This resolves the dilemma posed by the `eagerError` parameter on `Future.wait()`: if `false` then it could wait a long time to report an error, but if `true` then it gives no way to know when all tasks have finished. In contrast, `TaskGroup.waitAll()` waits until all tasks are done, but should return promptly after an error (if they handle cancellation suitably). Use it like this: 

```dart
Future<List<Uint8List>> remoteReads() async {
  return await TaskGroup.waitAll([
    (signal) => readBytes('alpha.example.com', 8080, 100, signal: signal),
    (signal) => readBytes('beta.example.com', 8080, 100, signal: signal),
  ]);
}
```

> [!NOTE]
> The file `bin/examples.dart` has two extra task group examples:
>
> * Happy eyeballs, the "hello world" of structured concurrency; starts multiple connection attempts staggered over time, picking the first to succeed
> * Nested task group server, showing how to nest groups; an outer one handles connections and an inner one handles listening ports

## `lib/task_group.dart`

Task group implementation

```dart
class AggregateException implements Exception {
  final List<Object> exceptions;
  final List<StackTrace> stackTraces;
}
```

Thrown by TaskGroup.waitComplete(), TaskGroup.using() and TaskGroup.waitAll() when one or more tasks fail. All the exceptions (except any `AbortException` instances) are collected into it, which ensures no failure is silently discarded.

```dart
class TaskGroup {
  TaskGroup({
    AbortSignal? parentSignal,
    Duration? timeout,
    bool raiseOnTimeout = true,
  });
  static Future<void> using({
    required Future<void> Function(TaskGroup) body,
    AbortSignal? parentSignal,
    Duration? timeout,
    bool raiseOnTimeout = true,
  });
  static Future<List<T>> waitAll<T>(
    Iterable<Future<T> Function(AbortSignal)> tasks, {
    AbortSignal? parentSignal,
    Duration? timeout,
    void Function(T)? cleanUp,
  });
  AbortSignal get signal;
  bool get completed;
  bool get didTimeout;
  // bool get abortCaught; - to do (like Trio's cancelled_caught)
  void abort();
  void spawn<T>(Future<T> Function(AbortSignal) task);
  Future<Outcome<T>> spawnWithFuture<T>(Future<T> Function(AbortSignal) task);
  Future<void> waitComplete();
}
```

Runs tasks and waits for them to complete. Use `spawn()` to start tasks and use `waitComplete()` to wait for them all to finish. So long as `waitComplete()` has not
yet returned, new tasks may continue to be spawned.
 If any task throws an exception then all tasks are cancelled (to be more
precise: the signal passed to all tasks is aborted), but `waitComplete()`
still waits for them all to finish, and new tasks may even still be spawned. 

> [!NOTE]
> A task group can raise the following exceptions, listed in priority order: 
>
> * `StateError` if a task throws `AbortException` despite its `AbortSignal` not being aborted (this is a programming error)
> * `AggregateException` if any task throws an exception other than `AbortException`
> * `AbortException` if the parent signal is aborted
> * `TimeoutException` if the specified timeout expires (and `raiseOnTimeout` is true, which is its default) 
> * Otherwise, no exception is raised, even if `TaskGroup.abort()` has been called (the task group consumes its own abort exceptions)

## `bin/examples.dart`

Additional task group examples.

## `bin/main.dart`

A scratch file for manually testing and exercising the code in the other files.


## Further development

This code is already useful as it stands. But there are a few potential additions that could further improve it (besides tests and documentation!):

* **New base class for `AbortException`:** In this code, `AbortException` derives from `Exception` but that means it will be caught by any blanket `catch (Exception)` blocks. Ideally, Dart would introduce a new `BaseException` class, a shared base for `Exception` and `Error`, and `AbortException` could derive from that directly. As a bonus, if Dart later puts traceback information in the exception (where it belongs!), or adds exception chaining, then this gives a central place to put it. (This is inspired by Python's `BaseException`, which asyncio and Trio both use as the direct base for their cancellation exceptions.)
* **Support cancellable HTTP and web sockets:** Cancellation tokens finally give a natural way to specify when `HttpClient.getUrl()` and others should abort (see https://github.com/dart-lang/sdk/issues/51267 – which actually suggests `AbortSignal`!). I haven't thought through the implications to the http API (e.g. should the cancel token passed to a request method be inherited by close() or should that take its own? And it looks like there needs to be a synchronous method to close a request that doesn't depend on whether it's got to response stage).  
* **Passing cancel tokens between isolates:** This is a bit more specialised, but would help with cancelling work delegated to other isolates. It would be a lot more intrusive than the other changes described here so it might be worth making it more general e.g. supporting a general [event object](https://en.wikipedia.org/wiki/Event_(computing)) passed between isolates (which is what a cancel token essentially is). This is safe so long as the destination can only read the value (i.e. for cancel tokens, you can only pass the signal not the controller) and so long as it can't be unset (uncancelled). It should be possible to see an update without an event loop iteration, so that it's possible to interrupt a long running CPU operation if the user's code polls it occasionally.

## License 

Copyright 2026 Arthur Tacca

Dual licensed. You may use under the terms of either:

* 3 clause BSD license
* Boost Software License 1.0

I'm happy to be considered as one of the "Dart contributors" for attribution purposes, and happy to sign a CLA if needed.
