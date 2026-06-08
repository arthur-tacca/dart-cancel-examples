# Dart Cancellation Proposal: Transparent Structured Concurrency Variation

This is an alternative variant of my main Dart cancellation examples.

* **Discussion:** [dart-lang/sdk#63017](https://github.com/dart-lang/sdk/issues/63017)
* **Main proposal and proof of concept:** https://github.com/arthur-tacca/dart-cancel-examples
* **Alternative approach (transparent structured concurrency):** https://github.com/arthur-tacca/dart-cancel-examples/tree/transparent-structured-concurrency

## Contents

* [Introduction](#introduction)
* [Examples from main branch](#examples-from-main-branch)
* [Issues](#issues)
* [Implementation](#implementation)
* [API Description](#api-description)

## Introduction

Structured concurrency, as introduced (in its modern form) by the Python 
library [Trio](https://trio.readthedocs.io) and used in many other languages 
since, is based around the concepts of **cancel scopes** for initiating 
cancellation and **task groups** (also called **nurseries**) for managing 
the lifetime of tasks. The main branch of this proposal repo has those, 
along with **cancel tokens** passed around in application code to propagate 
cancellation from the scope down to functions called within them. 

But the whole point of cancel scopes, originally, is to avoid cancel tokens! 
Cancellation is meant to be transparently propagated down to any functions 
called within a scope, and to any child tasks spawned in task groups within 
it. The point of this branch is to show that this is possible in Dart too. 
The following snippet shows real Dart code using facilities from this branch.
Notice how the `await sleep(...)` call does not need a cancel token parameter:

```dart
// Prints:
// Elapsed: 1010ms, cancelCaught: true
Future<void> timedSleepWithTimeout() async {
  final stopwatch = Stopwatch()..start();
  final scope = CancelScope(timeout: Duration(seconds: 1));
  await scope.using(() async {
    await sleep(Duration(seconds: 2));
  });
  stopwatch.stop();
  print('Elapsed: ${stopwatch.elapsedMilliseconds}ms, '
      'cancelCaught: ${scope.cancelCaught}');
}
```

The cancellation automatically propagates down to nested calls:

```dart
// Prints:
// Elapsed: 1001ms, cancelCaught: true
// (Does not print "Slept for..." from cancelled inner call)
Future<void> timedSleepAndPrintWithTimeout() async {
  final stopwatch = Stopwatch()..start();
  final scope = CancelScope(timeout: Duration(seconds: 1));
  await scope.using(() async {
    await sleepAndPrint(Duration(seconds: 2));
  });
  stopwatch.stop();
  print('Elapsed: ${stopwatch.elapsedMilliseconds}ms, '
      'cancelCaught: ${scope.cancelCaught}');
}
Future<void> sleepAndPrint(Duration duration) async {
  await sleep(duration);
  print('Slept for ${duration.inMilliseconds}ms');
}
```

The cancellation also propagates into all child tasks in any task group 
within a cancel scope (and grandchildren etc. recursively in nested task 
groups):

```dart
// Prints:
// Elapsed: 1002ms, cancelCaught: true
Future<void> timedTaskGroupWithCancel() async {
  final stopwatch = Stopwatch()..start();
  final scope = CancelScope(timeout: Duration(seconds: 1));
  await scope.using(() async {
    final tg = TaskGroup();
    await tg.using(() async {
      tg.spawn(() => sleep(Duration(milliseconds: 500)));
      tg.spawn(() => sleep(Duration(seconds: 2)));
    });
  });
  stopwatch.stop();
  print('Elapsed: ${stopwatch.elapsedMilliseconds}ms, '
      'cancelCaught: ${scope.cancelCaught}');
}
```

The above function uses a cancel scope with a task group inside it to make a 
point about propagation but, for the record, it's redundant because every task
group has its own cancel scope. It implicitly surrounds all tasks spawned 
within it, including the body passed to `using()`, and it's used to cancel 
the tasks if any task throws an exception. So the above example could be 
written more simply as:

```dart
// Prints:
// Elapsed: 1001ms, cancelCaught: true
Future<void> taskGroupWithOwnTimeout() async {
  final stopwatch = Stopwatch()..start();
  final tg = TaskGroup();
  tg.scope.setTimeout(Duration(seconds: 1));
  await tg.using(() async {
    tg.spawn(() => sleep(Duration(milliseconds: 500)));
    tg.spawn(() => sleep(Duration(seconds: 2)));
  });
  stopwatch.stop();
  print('Elapsed: ${stopwatch.elapsedMilliseconds}ms, '
      'cancelCaught: ${tg.scope.cancelCaught}');
}
```

Note that each task picks up cancellation from its own task group (and 
enclosing cancel scopes), not from the point in code where they're spawned. 
This is important because it's valid to spawn a task into a different task 
group. In the following example, code in a nested task group spawns a task 
into the parent task group, which continues executing even after the inner 
one is cancelled:

```dart
// Prints:
// Inner elapsed: 501ms, cancelCaught: true
// Outer elapsed: 1001ms, cancelCaught: false
Future<void> taskGroupOwnershipExample() async {
  final stopwatch = Stopwatch()..start();
  final outer = TaskGroup();
  await outer.using(() async {
    await spawnIntoBothGroups(outer);
  });
  stopwatch.stop();
  print('Outer elapsed: ${stopwatch.elapsedMilliseconds}ms, '
      'cancelCaught: ${outer.scope.cancelCaught}');
}
Future<void> spawnIntoBothGroups(TaskGroup outer) async {
  final stopwatch = Stopwatch()..start();
  final inner = TaskGroup();
  inner.scope.setTimeout(Duration(milliseconds: 500));
  await inner.using(() async {
    inner.spawn(() => sleep(Duration(seconds: 2)));
    outer.spawn(() => sleep(Duration(seconds: 1)));
  });
  stopwatch.stop();
  print('Inner elapsed: ${stopwatch.elapsedMilliseconds}ms, '
      'cancelCaught: ${inner.scope.cancelCaught}');
}
```

## Examples from main branch

This section has the examples from the main branch, adjusted for the 
implicit cancellation scheme of this branch.

The `readWithCancel()` function is very similar but without the explicit 
`cancelToken` argument to `readBytes()`:

```dart
Future<Uint8List> readWithCancel() async {
  final scope = CancelScope();
  mightCallLater(() => scope.cancel());
  Uint8List? result;
  await scope.using(() async {
    result = await readBytes('example.com', 8080, 100);
  });
  if (scope.cancelCaught) {
    throw MyException("Operation was interrupted");
  }
  return result!;
}
```

The `readBytes()` function is an example of code that supports being cancelled 
but doesn't initiate any cancellation itself. In the main branch, that just 
requires you to accept a token as a parameter and pass it down to each 
function that you call (and make sure you clean up suitably when exceptions 
are thrown); often it looks almost identical to a non cancel-aware function.
In this version, cancellation is propagated downwards automatically, so you 
don't even need to pass a token around; often, a function that supports 
cancellation really is identical to how it would be written before 
cancellation support was added:

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
that you'd use `currentCancelToken` to access the ambient cancellation token (from
the enclosing `CancelScope` or `TaskGroup`). For example, the implementation
of `connectSocket` looks like this:

```dart
Future<Socket> connectSocket(String host, int port) async {
  final token = currentCancelToken;
  token?.throwIfCancelled();
  final task = await Socket.startConnect(host, port);
  final registration = token?.register(task.cancel);
  try {
    return await task.socket;
  } on SocketException {
    if (token?.cancelled ?? false) {
      throw const CancelException();
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
presumably also cancels the operation if it hasn't completed):

```dart
Future<Data> doSomethingAndCleanup() async {
  Conn c = await createConnection();
  Future.delayed(const Duration(seconds: 1), c.cleanup);
  return await c.getData();
}
```

If `Future.delayed()` gains support for implicit cancellation, and this 
function gets called from within a `CancelScope` or task group, suddenly it
might start leaking resources or, more likely, terminating the program (due
to an unhandled `CancelException`).

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
called from a scope that later gets cancelled, may actually poison the
connection for any later users. Or, again, it might crash the program with 
an unhandled `CancelException`.

What these examples have in common is a sort of "cancellation-safe sandwich":

* The application creates a `CancelScope` or `TaskGroup` and uses it
  correctly.
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


## Implementation

As you can see from `connectSocket()` above, cancel tokens are still used 
under the hood. The difference is that, rather than being explicitly passed 
around, they are stored in a
[zone-local value](https://dart.dev/libraries/async/zones#storing-zone-local-values).
A private global marker variable acts as the key:

```dart
const Object _cancelTokenZoneKey = CancelTokenZoneKey._();
```

`CancelScope.using()` forks a child zone and sets the zone-local variable to
its cancel token, and runs the body in that zone:

```dart
Future<void> using(Future<void> Function() body) async {
  // ... (throws StateError if the scope is reused) ...
  try {
    await Zone.current
        .fork(zoneValues: {_cancelTokenZoneKey: _cancelToken})
        .run(body);
  } on CancelException {
    // ... absorb our own cancellation, or rethrow the parent's ...
  } finally {
    // ... cancel the timeout timer, unregister from the parent token ...
  }
}
```

As shown above, this can be read with `currentCancelToken`:

```dart
CancelToken? get currentCancelToken =>
    Zone.current[_cancelTokenZoneKey] as CancelToken?;
```

Task groups capture their own cancel scope's zone on construction, and then 
spawn child tasks into that zone:

```dart
class TaskGroup {
  // ... data members ...
  TaskGroup({bool shield = false}) : _scope = CancelScope(shield: shield) {
    _waitFuture = _scope.using(() async {
      _zone = Zone.current;
      await _tasksDone.future;
    });
    // ... some other admin ...
  }
  Future<Outcome<T>> spawnWithFuture<T>(Future<T> Function() task) {
    // ... reject spawns on a completed group, bump the pending count ...
    Future<Outcome<T>> wrap() async {
      // ... call await task(); capture its result ...
    }
    return _zone.run(wrap);
  }
}
```


## API Description


### `lib/cancel_core.dart`

```dart
class CancelException implements Exception {}

class StrayCancelError extends Error {}

class CancelTokenRegistration {
  void unregister();
}

class CancelToken {
  bool get cancelled;
  void throwIfCancelled();
  CancelTokenRegistration register(void Function() callback);
}

CancelToken? get currentCancelToken;

class CancelScope {
  CancelScope({
    bool shield = false,
    Duration? timeout,
  });
  bool get cancelCaught;
  bool get shield;
  void cancel();
  void setTimeout(Duration timeout);
  Future<void> using(Future<void> Function() body);
  static Future<T> withTimeout<T>({
    required Future<T> Function() body,
    required Duration timeout,
    bool shield = false,
  });
}
```

Similar to the main proposal, except that the ambient cancel token (of the 
enclosing `CancelScope`) is obtained with `currentCancelToken`.

The main change is the addition of the `shield` parameter. When set to true, 
this protects the code from outside cancellation. This is equivalent to, in 
the main proposal, not passing the `CancelToken` from the parent task group 
(and that is how it is implemented).

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

Future<void> sleep([Duration? duration]);

Stream<T> streamCancellable<T>(Stream<T> stream);

({Stream<T> stream, StreamSink<T> sink}) oneWayChannel<T>([
  bool drainOnCancel = true,
]);
```

These are similar to in the main proposal (with similar implementation) but 
with no explicit `CancelToken` parameter.

> [!TIP]
> Use `await sleep(Duration.zero)` for an explicit check for cancellation. 
> This is a common pattern in other async frameworks, and it avoids normal 
> application code from having to interact with `CancelToken` which, in 
> this version of the proposal, is only intended for low-level async 
> functions that actually need to implement cancellation. (Those functions 
> should use `currentCancelToken?.throwIfCancelled()` to check for cancellation 
> instead.)
>
> Use `await sleep()` (with no duration) to wait "forever" i.e. until the 
> enclosing scope is cancelled.

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

Again, similar to the original version, but without explicit `CancelToken` 
parameter.

### `lib/task_group.dart`

```dart
class AggregateException implements Exception {
  final List<Object> exceptions;
  final List<StackTrace> stackTraces;
}
class TaskGroup {
  TaskGroup({bool shield = false});
  bool get completed;
  CancelScope get scope;
  void spawn<T>(Future<T> Function() task);
  Future<Outcome<T>> spawnWithFuture<T>(Future<T> Function() task);
  Future<void> using(Future<void> Function() body);
  Future<void> waitComplete();
  static Future<List<T>> waitAll<T>(
    Iterable<Future<T> Function()> tasks, {
    bool shield = false,
    Duration? timeout,
    void Function(T)? cleanUp,
  });
  static Future<T> waitAny<T>(
    Iterable<Future<T> Function()> tasks, {
    bool shield = false,
    Duration? timeout,
    void Function(T)? cleanUp,
  });
}
```

Even the `TaskGroup` class has a very similar interface to the main proposal,
but without tokens being passed in or to spawned functions.
