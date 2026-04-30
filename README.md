# Dart cancel tokens code examples 

This repository contains example Dart code to accompany a proposal to add
cancellation tokens to the [Dart programming language](https://dart.dev). The
design is modelled on JavaScript's `AbortController`/`AbortSignal` and C#'s
`CancellationTokenSource`/`CancellationToken`.

- **Proposal:** https://gist.github.com/arthur-tacca/accbd333a6378619936e34d184b0d152
- **Discussion:** https://github.com/dart-lang/sdk/issues/63017
- **Code examples:** https://github.com/arthur-tacca/dart-cancel-examples

A typical usage looks like this, where `readBytes` is some routine that accepts
a cancel token:

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

***Disclaimer:** I'm very familiar with general cancellation concepts but new to
Dart so much of this code was written with AI help (with a lot of human guidance and
supervision ... except for main.dart, which I've hardly looked at).*

## `lib/abort.dart`

The core cancellation types.

```dart
class AbortController {
  AbortSignal get signal;
  void abort();
}
```
Creates and owns an `AbortSignal`; call `abort()` to cancel it.

```dart
class AbortSignal {
  bool get aborted;
  void throwIfAborted();
  AbortSignalRegistration register(void Function() callback);
  static AbortSignal timeout(Duration duration);
  static AbortSignal any(Iterable<AbortSignal> signals);
}  
```
The cancel token itself. Obtain one from an `AbortController` or the static
factory methods. `register()` schedules a callback as a microtask when the
signal aborts.

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

## `lib/utils.dart`

Example cancellable utility functions built on top of `abort.dart`.

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

```dart
Future<void> sleep(
  Duration duration, [
  AbortSignal? signal,
]);
```
Sleeps for the given duration, like 
[`Future.delayed`](https://api.dart.dev/dart-async/Future/Future.delayed.html)
(without the computation parameter). Can be interrupted by the abort signal.

```dart
Stream<T> streamCancellable<T>(
  Stream<T> stream,
  AbortSignal signal,
);
```
Wraps a [Stream](https://api.dart.dev/dart-async/Stream-class.html) so that an
`AbortException` is injected and the source subscription cancelled when the
signal aborts.

```dart
Future<List<T>> waitAll<T>(
  Iterable<Future<T> Function(AbortSignal)> tasks, {
  AbortSignal? signal,
  void Function(T)? cleanUp,
});
```
Runs tasks concurrently and waits for them all to finish, similar to
[`Future.wait()`](https://api.dart.dev/dart-async/Future/wait.html).

If any of the functions raises an exception then the others are aborted. This
resolves the dilemma posed by the `eagerError` parameter on `Future.wait()`: if
`false` then it could wait a long time to report an error, but if `true` then it
gives no way to know when all tasks have finished. This function only
returns when all tasks complete, but this should happen promptly after any error.
It also collects all exceptions (except `AbortException`) into an `AggregateException`, so unlike
`Future.wait()` none are discarded. This is inspired by 
[structured concurrency](https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/),
although this function is not quite as powerful as it lacks the ability to dynamically wait on 
more tasks once started. 

The tasks can also be interrupted by the optional `AbortSignal` parameter (but
this is not necessary for any of the above functionality). Unlike most cancellable
functions, this function is deliberately **not** a no-op if it is run with an
already-aborted signal; tasks are all still started, although with an
already-aborted signal passed to them too. This to allow the tasks to run to the
point that they can release any resources they are responsible for. It's similar
to how Trio ensures all tasks run until at least their first `await`
([but asyncio does not](https://github.com/python/cpython/issues/116048)).

Raises a `StateError` if any task raises `AbortException` when it is not aborted. 

```dart
class AggregateException implements Exception {
  final List<Object> exceptions;
  final List<StackTrace> stackTraces;
}
```
Thrown by the `waitAll` family when one or more tasks fail with a
non-`AbortException`. Similar to JavaScript
[`AggregateError`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/AggregateError),
C# [`AggregateException`](https://learn.microsoft.com/en-us/dotnet/api/system.aggregateexception),
Python [exception groups](https://docs.python.org/3/library/exceptions.html#exception-groups),
etc.


```dart
Future<void> waitAllSimple(
  Iterable<Future<void> Function(AbortSignal)> tasks, {
  AbortSignal? signal,
});
```
Simpler version of `waitAll` for tasks that do not return values. This is less
restrictive than it sounds, as tasks can store results in captured variables. 
This is the same reasoning given that tasks in
[Trio nurseries](https://trio.readthedocs.io/en/stable/reference-core.html#nurseries-and-spawning)
have no way to directly return a value.

```dart
Future<void> waitAllAlt<T>(
  Map<String, Future<T> Function(AbortSignal)> tasks, {
  AbortSignal? signal,
  Map<String, Outcome<T>>? results,
});
```
More elaborate version of `waitAll` where tasks are named by string key and
their individual outcomes are written into `results` before any exception is
thrown. Tasks that raise `AbortException` are excluded from `results`.

```dart
Future<Socket> connectSocket(
  String host,
  int port, {
  AbortSignal? abortSignal,
});
```
Cancellable TCP socket connection, built on
[`Socket.startConnect`](https://api.dart.dev/dart-io/Socket/startConnect.html).

## `bin/main.dart`

A scratch file for manually testing and exercising the code in the other two
files.
