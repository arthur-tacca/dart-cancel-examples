import 'dart:async';
import 'dart:io';
import 'package:dart_cancel_examples/cancel_core.dart';
import 'package:dart_cancel_examples/task_group.dart';
import 'package:dart_cancel_examples/cancellable.dart';
import 'package:dart_cancel_examples/networking.dart';

/// Yields to the microtask queue so scheduled callbacks can run.
Future<void> flushMicrotasks() => Future.microtask(() {});

void main() async {
  await demoBasicCancel();
  await demoRegisterUnregister();
  await demoAny();
  await demoTimeout();
  await demoWaitCancellable();
  await demoStreamCancellable();
  await demoSleep();
  await demoWaitAll();
  await demoWaitAny();
  await demoConnectSocket();
  await demoTaskGroup();
}

Future<void> demoBasicCancel() async {
  print('--- basic cancel ---');
  final controller = CancelController();
  final cancelToken = controller.cancelToken;

  cancelToken.register(() => print('cancelled!'));

  print('cancelled before: ${cancelToken.cancelled}');
  controller.cancel();
  print('cancelled after: ${cancelToken.cancelled}'); // true immediately
  await flushMicrotasks(); // 'cancelled!' prints here

  try {
    cancelToken.throwIfCancelled();
  } on CancelException {
    print('throwIfCancelled() threw CancelException');
  }
}

Future<void> demoRegisterUnregister() async {
  print('\n--- register / unregister before cancel ---');
  final controller = CancelController();
  final cancelToken = controller.cancelToken;

  final reg = cancelToken.register(() => print('this should not print'));
  reg.unregister();
  controller.cancel(); // callback was removed before cancel, nothing fires
  await flushMicrotasks();

  try {
    reg.unregister(); // second call — should throw StateError
  } on StateError catch (e) {
    print('double unregister caught: $e');
  }

  print('\n--- unregister after cancel, before microtask fires ---');
  final controller2 = CancelController();
  final cancelToken2 = controller2.cancelToken;

  final reg2 = cancelToken2.register(() => print('this should not print either'));
  controller2.cancel();            // schedules microtask
  reg2.unregister();              // unregistered before microtask fires
  await flushMicrotasks();        // microtask checks list != null, skips callback
  print('callback was suppressed');
}

Future<void> demoAny() async {
  print('\n--- CancelController linkedCancelTokens ---');
  final c1 = CancelController();
  final c2 = CancelController();
  final combined = CancelController(linkedCancelTokens: [c1.cancelToken, c2.cancelToken]);

  combined.cancelToken.register(() => print('combined fired'));

  print('combined cancelled before: ${combined.cancelToken.cancelled}');
  c2.cancel(); // schedules combined.cancel as microtask
  await flushMicrotasks(); // combined.cancel runs, token is cancelled, 'combined fired' scheduled
  await flushMicrotasks(); // 'combined fired' prints here
  print('combined cancelled after: ${combined.cancelToken.cancelled}');
}

Future<void> demoTimeout() async {
  print('\n--- CancelController timeout ---');
  final controller = CancelController(timeout: Duration(milliseconds: 300));
  controller.cancelToken.register(() => print('timed out'));

  print('cancelled before wait: ${controller.cancelToken.cancelled}');
  await Future.delayed(Duration(milliseconds: 500));
  print('cancelled after wait: ${controller.cancelToken.cancelled}');
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

  print('\n--- waitCancellable: cancelled before future completes ---');
  final controller = CancelController();
  final slowFuture = Future.delayed(Duration(milliseconds: 500), () => 99);
  Future.delayed(Duration(milliseconds: 100), controller.cancel);
  try {
    await waitCancellable(slowFuture, controller.cancelToken);
  } on CancelException {
    print('waitCancellable threw CancelException');
  }

  print('\n--- waitCancellable: already cancelled on entry ---');
  final controller2 = CancelController()..cancel();
  try {
    await waitCancellable(Future.value(1), controller2.cancelToken);
  } on CancelException {
    print('waitCancellable threw CancelException immediately');
  }
}

Future<void> demoSleep() async {
  print('\n--- sleep: completes normally ---');
  await sleep(Duration(milliseconds: 100));
  print('sleep completed');

  print('\n--- sleep: cancelled before completing ---');
  final controller = CancelController();
  Future.delayed(Duration(milliseconds: 50), controller.cancel);
  try {
    await sleep(Duration(milliseconds: 500), controller.cancelToken);
  } on CancelException {
    print('threw CancelException');
  }

  print('\n--- sleep: already cancelled ---');
  final controller2 = CancelController()..cancel();
  try {
    await sleep(Duration(milliseconds: 100), controller2.cancelToken);
  } on CancelException {
    print('threw CancelException immediately');
  }
}

Future<void> demoWaitAll() async {
  print('\n--- waitAll: all succeed ---');
  final results = await TaskGroup.waitAll([
    (cancelToken) async { await sleep(Duration(milliseconds: 100), cancelToken); return 1; },
    (cancelToken) async { await sleep(Duration(milliseconds: 50),  cancelToken); return 2; },
    (cancelToken) async { await sleep(Duration(milliseconds: 150), cancelToken); return 3; },
  ]);
  print('results: $results');

  print('\n--- waitAll: one fails, others cancelled ---');
  try {
    await TaskGroup.waitAll(<Future<int> Function(CancelToken)>[
      (cancelToken) async { await sleep(Duration(milliseconds: 200), cancelToken); return 1; },
      (cancelToken) async {
        await sleep(Duration(milliseconds: 50), cancelToken);
        throw Exception('task 2 failed');
      },
      (cancelToken) async { await sleep(Duration(milliseconds: 200), cancelToken); return 3; },
    ]);
  } on AggregateException catch (e) {
    print('AggregateException with ${e.exceptions.length} exception(s): '
        '${e.exceptions.first}');
  }

  print('\n--- waitAll: outer cancelToken cancelled ---');
  final controller = CancelController();
  Future.delayed(Duration(milliseconds: 50), controller.cancel);
  try {
    await TaskGroup.waitAll(
      [
        (cancelToken) async { await sleep(Duration(seconds: 10), cancelToken); return 1; },
        (cancelToken) async { await sleep(Duration(seconds: 10), cancelToken); return 2; },
      ],
      parentCancelToken: controller.cancelToken,
    );
  } on CancelException {
    print('threw CancelException');
  }

  print('\n--- waitAll: cleanUp called on successes when another fails ---');
  try {
    await TaskGroup.waitAll(<Future<String> Function(CancelToken)>[
      (cancelToken) async { await sleep(Duration(milliseconds: 50), cancelToken); return 'resource-A'; },
      (cancelToken) async {
        await sleep(Duration(milliseconds: 100), cancelToken);
        throw Exception('task 2 failed');
      },
    ], cleanUp: (v) => print('cleanUp called for: $v'));
  } on AggregateException {
    print('AggregateException thrown');
  }
}

Future<void> demoWaitAny() async {
  print('\n--- waitAny: first to finish wins ---');
  final result = await TaskGroup.waitAny([
    (cancelToken) async { await sleep(Duration(milliseconds: 100), cancelToken); return 'slow'; },
    (cancelToken) async { await sleep(Duration(milliseconds: 20),  cancelToken); return 'fast'; },
    (cancelToken) async { await sleep(Duration(milliseconds: 60),  cancelToken); return 'medium'; },
  ]);
  print('result: $result');

  print('\n--- waitAny: empty list throws ArgumentError ---');
  try {
    await TaskGroup.waitAny<int>([]);
    print('did not throw');
  } on ArgumentError catch (e) {
    print('threw ArgumentError: $e');
  }

  print('\n--- waitAny: parent cancelToken cancelled ---');
  final controller = CancelController();
  Future.delayed(Duration(milliseconds: 30), controller.cancel);
  try {
    await TaskGroup.waitAny(
      [
        (cancelToken) async { await sleep(Duration(seconds: 10), cancelToken); return 1; },
        (cancelToken) async { await sleep(Duration(seconds: 10), cancelToken); return 2; },
      ],
      parentCancelToken: controller.cancelToken,
    );
  } on CancelException {
    print('threw CancelException');
  }

  print('\n--- waitAny: task fails before any success ---');
  try {
    await TaskGroup.waitAny(<Future<int> Function(CancelToken)>[
      (cancelToken) async {
        await sleep(Duration(milliseconds: 20), cancelToken);
        throw Exception('task 1 failed');
      },
      (cancelToken) async { await sleep(Duration(milliseconds: 200), cancelToken); return 99; },
    ]);
  } on AggregateException catch (e) {
    print('AggregateException with ${e.exceptions.length} exception(s): '
        '${e.exceptions.first}');
  }

  print('\n--- waitAny: cleanUp on losing successes ---');
  // Both tasks ignore the cancellation token so both produce results. The second one
  // races in after the first has triggered cancel, so it must be cleaned up.
  final winner = await TaskGroup.waitAny<String>([
    (cancelToken) async {
      await Future.delayed(Duration(milliseconds: 20));
      return 'resource-A';
    },
    (cancelToken) async {
      await Future.delayed(Duration(milliseconds: 40));
      return 'resource-B';
    },
  ], cleanUp: (v) => print('cleanUp called for: $v'));
  print('winner: $winner');

  print('\n--- waitAny: cleanUp on all results when group throws ---');
  // A succeeds at ~20ms; B fails at ~40ms with a non-Cancel error.
  try {
    await TaskGroup.waitAny<String>([
      (cancelToken) async {
        await Future.delayed(Duration(milliseconds: 20));
        return 'resource-A';
      },
      (cancelToken) async {
        await Future.delayed(Duration(milliseconds: 40));
        throw Exception('task B failed');
      },
    ], cleanUp: (v) => print('cleanUp called for: $v'));
  } on AggregateException catch (e) {
    print('AggregateException: ${e.exceptions.first}');
  }

  print('\n--- waitAny: timeout fires before any task succeeds ---');
  try {
    await TaskGroup.waitAny(
      [
        (cancelToken) async { await sleep(Duration(seconds: 10), cancelToken); return 1; },
        (cancelToken) async { await sleep(Duration(seconds: 10), cancelToken); return 2; },
      ],
      timeout: Duration(milliseconds: 30),
    );
  } on TimeoutException {
    print('threw TimeoutException');
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

  print('\n--- connectSocket: already cancelled ---');
  final server2 = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  server2.listen((_) {});
  try {
    final controller = CancelController()..cancel();
    try {
      await connectSocket('127.0.0.1', server2.port,
          cancelToken: controller.cancelToken);
    } on CancelException {
      print('threw CancelException immediately');
    }
  } finally {
    await server2.close();
  }

  print('\n--- connectSocket: cancelled during connect ---');
  // 198.51.100.0/24 is RFC 5737 TEST-NET-2, reserved for documentation and
  // not routed on the public internet, so the SYN goes into the void and
  // the connect hangs long enough for the 100ms cancel to win. Port 81 is
  // used because port 80/443 are intercepted by transparent proxies in
  // some sandboxed environments. If the connect does somehow succeed,
  // destroy the socket so it doesn't keep the isolate alive past main().
  final cancelToken = CancelController(timeout: Duration(milliseconds: 100)).cancelToken;
  try {
    final socket = await connectSocket('198.51.100.1', 81, cancelToken: cancelToken);
    socket.destroy();
    print('unexpectedly connected; socket destroyed');
  } on CancelException {
    print('threw CancelException mid-connect');
  }
}

Future<void> demoTaskGroup() async {
  print('\n--- TaskGroup.using: all tasks succeed ---');
  final order = <int>[];
  await TaskGroup.using(body: (tg) async {
    tg.spawn((cancelToken) async {
      await sleep(Duration(milliseconds: 100), cancelToken);
      order.add(1);
    });
    tg.spawn((cancelToken) async {
      await sleep(Duration(milliseconds: 50), cancelToken);
      order.add(2);
    });
  });
  print('completed, finish order: $order'); // expect [2, 1]

  print('\n--- TaskGroup.using: one task fails, sibling cancelled ---');
  try {
    await TaskGroup.using(body: (tg) async {
      tg.spawn((cancelToken) async {
        await sleep(Duration(milliseconds: 200), cancelToken);
        order.add(99); // should not reach here
      });
      tg.spawn((cancelToken) async {
        await sleep(Duration(milliseconds: 50), cancelToken);
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
      tg.spawn((cancelToken) async {
        await sleep(Duration(milliseconds: 200), cancelToken);
      });
      throw Exception('body failed');
    });
  } on AggregateException catch (e) {
    print('AggregateException from body: ${e.exceptions}');
  }

  print('\n--- TaskGroup: spawnWithFuture returns individual result ---');
  final tg = TaskGroup();
  final fut = tg.spawnWithFuture((cancelToken) async {
    await sleep(Duration(milliseconds: 50), cancelToken);
    return 42;
  });
  await tg.waitComplete();
  print('individual result: ${(await fut).get()}');
  print('completed: ${tg.completed}');

  print('\n--- TaskGroup: waitComplete() called twice returns same future ---');
  final tg2 = TaskGroup();
  tg2.spawn((cancelToken) async => sleep(Duration(milliseconds: 50), cancelToken));
  final f1 = tg2.waitComplete();
  final f2 = tg2.waitComplete();
  await f1;
  print('same future: ${identical(f1, f2)}');

  print('\n--- TaskGroup: cancel() with no task errors completes normally ---');
  final tg3 = TaskGroup();
  tg3.spawn((cancelToken) async {
    await sleep(Duration(seconds: 10), cancelToken);
  });
  Future.delayed(Duration(milliseconds: 50), tg3.cancel);
  await tg3.waitComplete();
  print('completed normally, cancelToken cancelled: ${tg3.cancelToken.cancelled}');

  print('\n--- TaskGroup: spawn() after completed throws StateError ---');
  final tg4 = TaskGroup();
  await tg4.waitComplete();
  try {
    tg4.spawn((_) async {});
  } on StateError catch (e) {
    print('StateError: $e');
  }

  print('\n--- TaskGroup: cancel() after completed sets cancelToken.cancelled ---');
  final tg5 = TaskGroup();
  await tg5.waitComplete();
  print('cancelToken.cancelled before: ${tg5.cancelToken.cancelled}');
  tg5.cancel();
  print('cancelToken.cancelled after: ${tg5.cancelToken.cancelled}');

  print('\n--- TaskGroup: parentCancelToken cancels the group ---');
  final controller = CancelController();
  Future.delayed(Duration(milliseconds: 50), controller.cancel);
  try {
    await TaskGroup.using(parentCancelToken: controller.cancelToken, body: (tg) async {
      tg.spawn((cancelToken) async {
        await sleep(Duration(seconds: 10), cancelToken);
      });
    });
  } on CancelException {
    print('threw CancelException from parentCancelToken');
  }

  print('\n--- TaskGroup: timeout fires, raiseOnTimeout=true ---');
  try {
    await TaskGroup.using(timeout: Duration(milliseconds: 50), body: (tg) async {
      tg.spawn((cancelToken) async {
        await sleep(Duration(seconds: 10), cancelToken);
      });
    });
  } on TimeoutException {
    print('threw TimeoutException');
  }

  print('\n--- TaskGroup: timeout fires, raiseOnTimeout=false ---');
  final tg6 = TaskGroup(timeout: Duration(milliseconds: 50), raiseOnTimeout: false);
  tg6.spawn((cancelToken) async {
    await sleep(Duration(seconds: 10), cancelToken);
  });
  await tg6.waitComplete();
  print('completed normally, didTimeout: ${tg6.didTimeout}');

  print('\n--- TaskGroup: setTimeout() replaces timeout mid-flight ---');
  try {
    await TaskGroup.using(body: (tg) async {
      tg.spawn((cancelToken) async {
        await sleep(Duration(seconds: 10), cancelToken);
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
}

Future<void> demoStreamCancellable() async {
  print('\n--- streamCancellable: cancelled mid-stream ---');
  final controller = CancelController();

  // Stream that emits 1, 2, 3 with 200ms gaps
  final source = Stream.periodic(Duration(milliseconds: 200), (i) => i + 1)
      .take(5);

  // Cancel after 450ms — should receive 1 and 2, then CancelException
  Future.delayed(Duration(milliseconds: 450), controller.cancel);

  try {
    await for (final value in streamCancellable(source, controller.cancelToken)) {
      print('received: $value');
    }
  } on CancelException {
    print('stream threw CancelException');
  }

  print('\n--- streamCancellable: already cancelled on entry ---');
  final controller2 = CancelController()..cancel();
  try {
    await for (final _ in streamCancellable(Stream.value(1), controller2.cancelToken)) {}
  } on CancelException {
    print('stream threw CancelException immediately');
  }

  print('\n--- streamCancellable: source completes before cancel ---');
  final controller3 = CancelController();
  final values = <int>[];
  await for (final v in streamCancellable(
    Stream.fromIterable([10, 20, 30]),
    controller3.cancelToken,
  )) {
    values.add(v);
  }
  print('completed normally, values: $values');
}
