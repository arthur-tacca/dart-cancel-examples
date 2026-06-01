# More about cancelling async generators

As the main README says, the `streamCancellable()` function is not suitable for
use with async generators (functions with the `async*` keyword). It works by 
converting *token cancellation* into *stream cancellation*, which works well 
for streams that promptly handle stream cancellation, but async generators 
do not. This page explains why.

## The problem

To see the problem, look at the typical pattern for an async generator:

```dart
Stream<Item> generateItems() async* {
  while (true) {
    final item = await getNext();
    yield item;
  }
}
Future<void> processItems({CancelToken? cancelToken}) async {
  await for (final item in streamCancellable(generateItems(), cancelToken)) {
    await process(item, cancelToken: cancelToken);
  }
}
```

`streamCancellable()` turns cancellation of the token into cancellation of the
stream subscription, but an `async*` generator only acts on that when it next
reaches a `yield`. In effect, it's as if the generator code is a little like
this pseudocode (which uses an imaginary `thisGenerator` interface to generator
operations):

```dart
Stream<Item> generateItems() async* {
  while (true) {
    final item = await getNext();
    if (thisGenerator.isCancelled) {  // if cancelled by streamCancellable()
      return;
    }
    thisGenerator.deliverItem(item);  // next item in for loop iteration
    await thisGenerator.resumes();    // wait for body of loop to run
    if (thisGenerator.isCancelled) {  // if body of loop breaks/returns/throws
      return;
    }
  }
}
```

That is problematic in two ways:

* Cancelling the stream does not cancel the in-progress `getNext()` call, 
  which is usually the very thing you wanted to interrupt.
* When `getNext()` eventually returns a value, the generator returns before
  it gets a chance to deliver the item to the for loop. (In other words, it
  is silently discarded by `yield item`.)

## The solution: cancel tokens

The solution is to rewrite the async generator to use cancel tokens directly:

```dart
Stream<Item> generateItems({CancelToken? cancelToken}) async* {
  while (true) {
    final item = await getNext(cancelToken: cancelToken);
    yield item;
  }
}
Future<void> processItems({CancelToken? cancelToken}) async {
  await for (final item in generateItems(cancelToken: cancelToken)) {
    await process(item, cancelToken: cancelToken);
  }
}
```

The analogous pseudocode this time is:

```dart
Stream<Item> generateItems({CancelToken? cancelToken}) async* {
  while (true) {
    final item = await getNext(cancelToken: cancelToken);
    thisGenerator.deliverItem(item);  // next item in for loop iteration
    await thisGenerator.resumes();    // wait for body of loop to run
    if (thisGenerator.isCancelled) {  // if body of loop breaks/returns/throws
      return;
    }
  }
}
```

Now the only points at which the function can return are:

* The `getNext(...)` function detects token cancellation and throws
  `CancelException` (or throws an exception for some other reason). In that
  case, it's the function's responsibility to make sure it doesn't leak any
  resource internally.
* The stream can only be cancelled (causing `yield item` to return from the
  generator function) if the body of the for loop breaks/returns/throws.
  Critically, this can only happen after the item has been delivered to the
  loop.

## Exception propagation

For completeness, this is how exceptions raised by `generateItems()` and
`processItems()` propagate to each other:

* **Body of `await for` throws exception:** `yield item` always *returns* 
  from the async generator function (rather than throws) even if the body of 
  the `await for` loop has thrown an exception (e.g. `CancelException`).
* **Generator function throws exception (when the for loop is waiting for an 
  item from it):** `await for` always *throws* any exception thrown by the 
  generator (e.g. `CancelException` originating from `getNext()`), which is 
  what we want for cancellation.
* **Both throw (originating in body of `await for`):** If the body of the 
  `await for` loop throws an exception, but then the generator function 
  throws an exception while it's handling that (e.g. it throws from a 
  `finally` block) then the new exception wins. This is a little like how a 
  finally block in a regular function can overrule a normally thrown 
  exception (except that the async generator has no way to see what the 
  exception from the loop body was).


When using `streamCancellable()`, the loop and generator can also finish due to the cancel token being cancelled, with the execution proceeding as follows:

* If the `await for` loop is currently waiting for the next element (i.e. 
  the async generator function is *not* currently waiting at a `yield`): 
  next time the async generator reaches a `yield` (if it doesn't return or 
  throw before then) then the `yield` acts like a return statement, and the 
  item passed to it is discarded.
  * If no exception is thrown from the async generator then the for loop 
    throws `CancelException`
  * If an exception is thrown (either before reaching a `yield`, or in a 
    `finally` block) then that is thrown out of the for loop (taking 
    precedence over the automatically injected `CancelException`)
* If the `await for` loop is currently executing the loop body (i.e. the 
  async generator function is waiting at a `yield`): the async generator
  function is not resumed until the loop body finishes
  (`streamCancellable()` defers cancelling the stream until then). When the
  loop body does complete:
  * If the loop body completes normally or with `continue` (so the loop goes 
    on to ask for the next element): the for loop throws according to the 
    above rules for the async generator (i.e. whatever it throws, otherwise 
    `CancelException`).
  * If the loop body exits with `break` or `return` (so the loop stops 
    reading from the stream): the `break`/`return` takes effect and the loop 
    throws nothing ... unless the async generator throws while it is being 
    torn down, in which case that exception is thrown out of the loop (just 
    as for a raw `await for` that breaks out of a generator whose `finally` 
    throws). 
  * If the loop body throws an exception (very commonly `CancelException` 
    caused by the same token as passed to `streamCancellable`) then the loop 
    waits for the async generator to finish. If the generator throws while 
    it is being torn down then that exception wins; otherwise the loop 
    rethrows its own exception, which therefore takes precedence over the 
    automatically injected `CancelException`. Note that the generator's
    teardown exception takes precedence over the loop body's exception, just 
    as for exceptions not caused by `streamCancellable`.
