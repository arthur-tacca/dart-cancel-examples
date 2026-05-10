# Dart structured concurrency

This is a variant of my main Dart cancellation examples.

- **Original version:** https://github.com/arthur-tacca/dart-cancel-examples
- **This branch:** https://github.com/arthur-tacca/dart-cancel-examples/tree/transparent-structured-concurrency

In the main branch, cancellation is done through cancellation tokens (of 
type `AbortSignal`, following the JS naming). There is a `TaskGroup` class, 
which enables structured concurrency, but the caller still needs to pass
cancellation tokens around everywhere. It's not a disaster but does add a 
little visual noise and there's always the risk of an accidentally omitted 
parameter.

In this branch, cancellation is propagated *transparently* from `TaskGroup` to 
all spawned routines and the routines they call and child task groups. No 
cancellation token parameters are needed anywhere.

Cancellation tokens are still used under the hood, but only as part of the 
implementation. Only the lowest-level routines, implementing individual IO 
calls, basic wait facilities or sychronisation primitives need to interact 
with cancellation tokens. Dart
[zones](https://dart.dev/libraries/async/zones) are used to implement 
transparent cancellation propagation (each task group opens a new zone, and 
stores its cancellation token as a zone-local variable).


## Summary and usage

In the main proposal, if you might want to cancel some code then you need to
either create a `AbortController` or (better) create a `TaskGroup`; 
either way, you can then call `abort()` later to interrupt the code 
controlled by it. In this version, your *only* option is a task group. So
the first usage example becomes:

```dart
Future<Uint8List> remoteRead() async {
  Uint8List? result;
  await TaskGroup.using(body: (tg) async {
    mightCallLater(tg.abort);
    result = await readBytes('example.com', 8080, 100);
  });
  if (result == null) {
    // The body was aborted before result was assigned
    throw MyException("Operation was interrupted");
  }
  return result!;
}
```

> [!NOTE]
> If [issue #1](https://github.com/arthur-tacca/dart-cancel-examples/issues/1) is implemented you would be able to use a cancel scope directly:
> 
> ```dart
> Future<Uint8List> remoteRead() async > {
>   Uint8List? result;
>   final scope = AbortScope();
>   mightCallLater(scope.abort);
>   await scope.using(body: () async {
>     result = await readBytes('example.com', 8080, 100);
>   });
>   if (scope.abortCaught) {
>     throw MyException("Operation was interrupted");
>   }
>   return result!;
> }

If you're writing a function that supports being cancelled then the cancel 
token proposal just requires you to accept a token as a parameter and pass 
it down to each function that you call (and make sure you clean up suitably 
when exceptions are thrown). Often it looks almost identical to a 
non cancel-aware function. In this version, cancellation is propagated 
downwards automatically, so you don't even need to pass a token around; 
often, a function that supports cancellation really is identical to 
how it would be written before cancellation support was added. The 
second usage example becomes:

```dart
Future<Uint8List> readBytes(String host, int port, int byteCount) async {
  final socket = await connectSocket(host, port);
  try {
    final buffer = BytesBuilder(copy: false);
    await for (final chunk in streamCancellable(socket)) {
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

*[If this were fully integrated into the language then you would not even 
need to specify `streamCancellable(...)` like above. Instead, stream 
iteration with `await for` would always be cancellable, so you'd simply write
`await for (final chunk in socket)`.]*

If you're implementing a leaf function, which actually implements some IO 
operation or synchronisation primitive, then you would still use the same 
cancellation token interface as in the main proposal. The only difference is 
that you'd use `currentSignal` to access the ambient cancellation token (from
the enclosing task group). For example, the implementation of `connectSocket` 
looks like this:

```dart
Future<Socket> connectSocket(String host, int port) async {
  final signal = currentSignal;
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
```

## Issues

This code is much cleaner than the main variant of the code, where cancel 
tokens have to be threaded through all function calls explicitly. However, 
this version comes with some backward compatibility issues that are probably 
insurmountable.

For example, here is a function that does some cleanup after a delay (which 
presumably also aborts the operation if it hasn't completed):

```dart
Future<Data> doSomethingAndCleanup() async {
  Conn c = await createConnection();
  Future.delayed(const Duration(seconds: 1), c.cleanup);
  return await c.getData();
}
```

If `Future.delayed()` gains support for implicit cancellation, and this 
function gets called from within a task group, suddenly it might start 
leaking resources or, more likely, terminating the program (due to an
unhandled `AbortException`).

Another example would be where a `Future` is spawned in the background with 
the expectation that it remains available for much later use:

```dart
class ReusableConnection {
  Future<void>? _impl;

  Future<Data> getData() {
    if (_impl == null) {
      _impl = _openConnection();
    }
    // ... use data passed from _impl (perhaps via a stream) ...
  }
}
```

In this example, an apparently innocent call to `await conn.getData()`, if 
called from a task group that later gets aborted, may actually poison the 
connection for any later users. Or, again, it might crash the program with 
an unhandled `AbortException`.

What these examples have in common is a sort of "cancellation-safe sandwich":

* The application creates a `TaskGroup` and uses it correctly.
* It calls some existing API, perhaps in a third party library, written 
  before structured concurrency was added.
* That calls lower-level functions in the standard library that have had 
  cancellation support added to them.

This risk doesn't appear in the main proposal because the lower-level 
functions only ever get cancelled if they have a token passed to them, which 
the old cancellation-unaware functions wouldn't do. What's more, the new 
application code can easily tell that the old function doesn't support 
cancellation (yet) because it doesn't accept a cancellation token parameter; 
this gives the caller a chance to plan for this fact.


## API Description


### `lib/abort.dart`

```dart
class AbortException implements Exception {}

class AbortSignalRegistration {
  void unregister();
}

class AbortSignal {
  bool get aborted;
  void throwIfAborted();
  AbortSignalRegistration register(void Function() callback);
}

AbortSignal? get currentSignal;
```

Similar to the main proposal, but `AbortController` is not intended to be 
used (except by `TaskGroup`).

### `lib/cancellable.dart`

```dart
class Outcome<T> {
  final bool success;
  final T? result;
  final Object? exception;
  final StackTrace? stackTrace;
  T get();
}

Future<Outcome<T>> waitCancellable<T>(Future<T> future);

Future<void> sleep(Duration duration);
```

These are similar to in the main proposal (with similar implementation) but 
with no explicit `AbortSignal` parameter.

> [!TIP]
> Use `await sleep(Duration.zero)` for an explicit check for cancellation. 
> This is a common pattern in other async frameworks, and it avoids normal 
> application code from having to interact with `AbortSignal` which, in 
> this version of the proposal, is only intended for low-level async 
> functions that actually need to implement cancellation. (Those functions 
> should use `currentSignal?.throwIfAborted()` to check for cancellation 
> instead.)

### `lib/networking.dart`

```dart
Future<Socket> connectSocket(String host, int port);

Future<SecureSocket> connectSecureSocket(
  String host,
  int port, {
  SecurityContext? context,
  bool Function(X509Certificate)? onBadCertificate,
});
```

Again, similar to the original version, but without explict `AbortSignal` 
parameter.

### `lib/task_group.dart`

```dart
class AggregateException implements Exception {
  final List<Object> exceptions;
  final List<StackTrace> stackTraces;
}
class TaskGroup {
  TaskGroup({
    bool shield = false,
    Duration? timeout,
    bool raiseOnTimeout = true,
  });
  static Future<void> using({
    required Future<void> Function(TaskGroup) body,
    bool shield = false,
    Duration? timeout,
    bool raiseOnTimeout = true,
  });
  static Future<List<T>> waitAll<T>(
    Iterable<Future<T> Function()> tasks, {
    bool shield = false,
    Duration? timeout,
    void Function(T)? cleanUp,
  });
  AbortSignal get signal;
  bool get completed;
  bool get didTimeout;
  bool get shield;
  void abort();
  void spawn<T>(Future<T> Function() task);
  Future<Outcome<T>> spawnWithFuture<T>(Future<T> Function() task);
  Future<void> waitComplete();
}
```

Even the `TaskGroup` class has a very similar interface to the main proposal,
but without signals being passed in or to spawned functions.

The main change is the addition of the `shield` parameter. When set to true, 
this protects the code from outside cancellation. This is equivalent to, in 
the main proposal, not passing the `AbortSignal` from the parent task group 
(and that is exactly how it is implemented).
