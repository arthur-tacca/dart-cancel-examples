import 'dart:async';
import 'dart:io';
import 'package:dart_cancel_examples/abort.dart';
import 'package:dart_cancel_examples/task_group.dart';
import 'package:dart_cancel_examples/cancellable.dart';
import 'package:dart_cancel_examples/networking.dart';

/// Yields to the microtask queue so scheduled callbacks can run.
Future<void> flushMicrotasks() => Future.microtask(() {});

void main() async {
  await demoBasicAbort();
  await demoRegisterUnregister();
  await demoWaitCancellable();
  await demoStreamCancellable();
  await demoSleep();
  await demoWaitAll();
  await demoConnectSocket();
  await demoTaskGroup();
}

Future<void> demoBasicAbort() async {
  print('--- basic abort ---');
  final controller = AbortController();
  final signal = controller.signal;

  signal.register(() => print('aborted!'));

  print('aborted before: ${signal.aborted}');
  controller.abort();
  print('aborted after: ${signal.aborted}'); // true immediately
  await flushMicrotasks(); // 'aborted!' prints here

  try {
    signal.throwIfAborted();
  } on AbortException {
    print('throwIfAborted() threw AbortException');
  }
}

Future<void> demoRegisterUnregister() async {
  print('\n--- register / unregister before abort ---');
  final controller = AbortController();
  final signal = controller.signal;

  final reg = signal.register(() => print('this should not print'));
  reg.unregister();
  controller.abort(); // callback was removed before abort, nothing fires
  await flushMicrotasks();

  try {
    reg.unregister(); // second call — should throw StateError
  } on StateError catch (e) {
    print('double unregister caught: $e');
  }

  print('\n--- unregister after abort, before microtask fires ---');
  final controller2 = AbortController();
  final signal2 = controller2.signal;

  final reg2 = signal2.register(() => print('this should not print either'));
  controller2.abort();            // schedules microtask
  reg2.unregister();              // unregistered before microtask fires
  await flushMicrotasks();        // microtask checks list != null, skips callback
  print('callback was suppressed');
}

Future<void> demoWaitCancellable() async {
  print('\n--- waitCancellable: future completes normally ---');
  final result = await waitCancellable(Future.value(42));
  print('returned: ${result.success}, value: ${result.get()}');

  print('\n--- waitCancellable: future throws ---');
  final result2 = await waitCancellable(
    Future.error(Exception('oops')),
  );
  print('returned: ${result2.success}');
  try {
    result2.get();
  } on Exception catch (e) {
    print('get() rethrew: $e');
  }

  print('\n--- waitCancellable: aborted before future completes ---');
  final slowFuture = Future.delayed(Duration(milliseconds: 500), () => 99);
  await TaskGroup.using(body: (tg) async {
    Future.delayed(Duration(milliseconds: 100), tg.abort);
    try {
      await waitCancellable(slowFuture);
    } on AbortException {
      print('waitCancellable threw AbortException');
    }
  });

  print('\n--- waitCancellable: already aborted on entry ---');
  await TaskGroup.using(body: (tg) async {
    tg.abort();
    try {
      await waitCancellable(Future.value(1));
    } on AbortException {
      print('waitCancellable threw AbortException immediately');
    }
  });
}

Future<void> demoSleep() async {
  print('\n--- sleep: completes normally ---');
  await sleep(Duration(milliseconds: 100));
  print('sleep completed');

  print('\n--- sleep: aborted before completing ---');
  await TaskGroup.using(body: (tg) async {
    Future.delayed(Duration(milliseconds: 50), tg.abort);
    try {
      await sleep(Duration(milliseconds: 500));
    } on AbortException {
      print('threw AbortException');
    }
  });

  print('\n--- sleep: already aborted ---');
  await TaskGroup.using(body: (tg) async {
    tg.abort();
    try {
      await sleep(Duration(milliseconds: 100));
    } on AbortException {
      print('threw AbortException immediately');
    }
  });
}

Future<void> demoWaitAll() async {
  print('\n--- waitAll: all succeed ---');
  final results = await TaskGroup.waitAll([
    () async { await sleep(Duration(milliseconds: 100)); return 1; },
    () async { await sleep(Duration(milliseconds: 50));  return 2; },
    () async { await sleep(Duration(milliseconds: 150)); return 3; },
  ]);
  print('results: $results');

  print('\n--- waitAll: one fails, others cancelled ---');
  try {
    await TaskGroup.waitAll(<Future<int> Function()>[
      () async { await sleep(Duration(milliseconds: 200)); return 1; },
      () async {
        await sleep(Duration(milliseconds: 50));
        throw Exception('task 2 failed');
      },
      () async { await sleep(Duration(milliseconds: 200)); return 3; },
    ]);
  } on AggregateException catch (e) {
    print('AggregateException with ${e.exceptions.length} exception(s): '
        '${e.exceptions.first}');
  }

  print('\n--- waitAll: outer signal aborted ---');
  await TaskGroup.using(body: (outer) async {
    Future.delayed(Duration(milliseconds: 50), outer.abort);
    try {
      await TaskGroup.waitAll([
        () async { await sleep(Duration(seconds: 10)); return 1; },
        () async { await sleep(Duration(seconds: 10)); return 2; },
      ]);
    } on AbortException {
      print('threw AbortException');
    }
  });

  print('\n--- waitAll: cleanUp called on successes when another fails ---');
  try {
    await TaskGroup.waitAll(<Future<String> Function()>[
      () async { await sleep(Duration(milliseconds: 50)); return 'resource-A'; },
      () async {
        await sleep(Duration(milliseconds: 100));
        throw Exception('task 2 failed');
      },
    ], cleanUp: (v) => print('cleanUp called for: $v'));
  } on AggregateException {
    print('AggregateException thrown');
  }
}

Future<void> demoConnectSocket() async {
  print('\n--- connectSocket: normal connect ---');
  final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  server.listen((_) {});
  try {
    final socket = await connectSocket('127.0.0.1', server.port);
    print('connected to port ${socket.remotePort}');
    socket.destroy();
  } finally {
    await server.close();
  }

  print('\n--- connectSocket: already aborted ---');
  final server2 = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  server2.listen((_) {});
  try {
    await TaskGroup.using(body: (tg) async {
      tg.abort();
      try {
        await connectSocket('127.0.0.1', server2.port);
      } on AbortException {
        print('threw AbortException immediately');
      }
    });
  } finally {
    await server2.close();
  }

  print('\n--- connectSocket: aborted during connect ---');
  // 10.0.0.1 is non-routable here so the connect hangs, letting the timeout win
  await TaskGroup.using(
    timeout: Duration(milliseconds: 100),
    raiseOnTimeout: false,
    body: (_) async {
      try {
        await connectSocket('10.0.0.1', 80);
      } on AbortException {
        print('threw AbortException mid-connect');
      }
    },
  );
}

Future<void> demoTaskGroup() async {
  print('\n--- TaskGroup.using: all tasks succeed ---');
  final order = <int>[];
  await TaskGroup.using(body: (tg) async {
    tg.spawn(() async {
      await sleep(Duration(milliseconds: 100));
      order.add(1);
    });
    tg.spawn(() async {
      await sleep(Duration(milliseconds: 50));
      order.add(2);
    });
  });
  print('completed, finish order: $order'); // expect [2, 1]

  print('\n--- TaskGroup.using: one task fails, sibling cancelled ---');
  try {
    await TaskGroup.using(body: (tg) async {
      tg.spawn(() async {
        await sleep(Duration(milliseconds: 200));
        order.add(99); // should not reach here
      });
      tg.spawn(() async {
        await sleep(Duration(milliseconds: 50));
        throw Exception('task failed');
      });
    });
  } on AggregateException catch (e) {
    print('AggregateException: ${e.exceptions}');
    print('sibling was cancelled (99 not in order): ${!order.contains(99)}');
  }

  print('\n--- TaskGroup.using: body itself throws ---');
  try {
    await TaskGroup.using(body: (tg) async {
      tg.spawn(() async {
        await sleep(Duration(milliseconds: 200));
      });
      throw Exception('body failed');
    });
  } on AggregateException catch (e) {
    print('AggregateException from body: ${e.exceptions}');
  }

  print('\n--- TaskGroup: spawnWithFuture returns individual result ---');
  final tg = TaskGroup();
  final fut = tg.spawnWithFuture(() async {
    await sleep(Duration(milliseconds: 50));
    return 42;
  });
  await tg.waitComplete();
  print('individual result: ${(await fut).get()}');
  print('completed: ${tg.completed}');

  print('\n--- TaskGroup: waitComplete() called twice returns same future ---');
  final tg2 = TaskGroup();
  tg2.spawn(() async => sleep(Duration(milliseconds: 50)));
  final f1 = tg2.waitComplete();
  final f2 = tg2.waitComplete();
  await f1;
  print('same future: ${identical(f1, f2)}');

  print('\n--- TaskGroup: abort() with no task errors completes normally ---');
  final tg3 = TaskGroup();
  tg3.spawn(() async {
    await sleep(Duration(seconds: 10));
  });
  Future.delayed(Duration(milliseconds: 50), tg3.abort);
  await tg3.waitComplete();
  print('completed normally, signal aborted: ${tg3.signal.aborted}');

  print('\n--- TaskGroup: spawn() after completed throws StateError ---');
  final tg4 = TaskGroup();
  await tg4.waitComplete();
  try {
    tg4.spawn(() async {});
  } on StateError catch (e) {
    print('StateError: $e');
  }

  print('\n--- TaskGroup: abort() after completed sets signal.aborted ---');
  final tg5 = TaskGroup();
  await tg5.waitComplete();
  print('signal.aborted before: ${tg5.signal.aborted}');
  tg5.abort();
  print('signal.aborted after: ${tg5.signal.aborted}');

  print('\n--- TaskGroup: parent (ambient) signal aborts the group ---');
  await TaskGroup.using(body: (outer) async {
    Future.delayed(Duration(milliseconds: 50), outer.abort);
    try {
      await TaskGroup.using(body: (inner) async {
        inner.spawn(() async {
          await sleep(Duration(seconds: 10));
        });
      });
    } on AbortException {
      print('threw AbortException from parent signal');
    }
  });

  print('\n--- TaskGroup: timeout fires, raiseOnTimeout=true ---');
  try {
    await TaskGroup.using(timeout: Duration(milliseconds: 50), body: (tg) async {
      tg.spawn(() async {
        await sleep(Duration(seconds: 10));
      });
    });
  } on TimeoutException {
    print('threw TimeoutException');
  }

  print('\n--- TaskGroup: timeout fires, raiseOnTimeout=false ---');
  final tg6 = TaskGroup(timeout: Duration(milliseconds: 50), raiseOnTimeout: false);
  tg6.spawn(() async {
    await sleep(Duration(seconds: 10));
  });
  await tg6.waitComplete();
  print('completed normally, didTimeout: ${tg6.didTimeout}');

  print('\n--- TaskGroup: setTimeout() replaces timeout mid-flight ---');
  try {
    await TaskGroup.using(body: (tg) async {
      tg.spawn(() async {
        await sleep(Duration(seconds: 10));
      });
      await sleep(Duration(milliseconds: 50));
      // Replace with a short timeout after some initial work
      tg.setTimeout(Duration(milliseconds: 50));
    });
  } on TimeoutException {
    print('threw TimeoutException from setTimeout');
  }

  print('\n--- TaskGroup: setTimeout() after completed is a no-op ---');
  final tg7 = TaskGroup();
  await tg7.waitComplete();
  tg7.setTimeout(Duration(milliseconds: 1));
  await sleep(Duration(milliseconds: 10));
  print('no error thrown');

  print('\n--- TaskGroup: shield blocks parent abort from reaching child ---');
  await TaskGroup.using(body: (outer) async {
    outer.abort();   // outer is aborted from the start
    print('outer.signal.aborted: ${outer.signal.aborted}');
    // A non-shielded child would inherit the parent abort and abort
    // immediately. A shielded one runs free.
    await TaskGroup.using(shield: true, body: (inner) async {
      print('  inner.shield: ${inner.shield}');
      print('  inner.signal.aborted before sleep: ${inner.signal.aborted}');
      await sleep(Duration(milliseconds: 50));   // would throw if aborted
      print('  shielded sleep completed');
    });
    print('outer body resumed; outer.signal.aborted still: '
        '${outer.signal.aborted}');
  });
}

Future<void> demoStreamCancellable() async {
  print('\n--- streamCancellable: aborted mid-stream ---');
  // Stream that emits 1, 2, 3 with 200ms gaps
  final source = Stream.periodic(Duration(milliseconds: 200), (i) => i + 1)
      .take(5);
  await TaskGroup.using(body: (tg) async {
    // Abort after 450ms — should receive 1 and 2, then AbortException
    Future.delayed(Duration(milliseconds: 450), tg.abort);
    try {
      await for (final value in streamCancellable(source)) {
        print('received: $value');
      }
    } on AbortException {
      print('stream threw AbortException');
    }
  });

  print('\n--- streamCancellable: already aborted on entry ---');
  await TaskGroup.using(body: (tg) async {
    tg.abort();
    try {
      await for (final _ in streamCancellable(Stream.value(1))) {}
    } on AbortException {
      print('stream threw AbortException immediately');
    }
  });

  print('\n--- streamCancellable: source completes before abort ---');
  final values = <int>[];
  await TaskGroup.using(body: (_) async {
    await for (final v in streamCancellable(Stream.fromIterable([10, 20, 30]))) {
      values.add(v);
    }
  });
  print('completed normally, values: $values');
}
