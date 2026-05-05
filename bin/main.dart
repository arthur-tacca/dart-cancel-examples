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
  await demoAny();
  await demoTimeout();
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

Future<void> demoAny() async {
  print('\n--- AbortController linkedSignals ---');
  final c1 = AbortController();
  final c2 = AbortController();
  final combined = AbortController(linkedSignals: [c1.signal, c2.signal]);

  combined.signal.register(() => print('combined fired'));

  print('combined aborted before: ${combined.signal.aborted}');
  c2.abort(); // schedules combined.abort as microtask
  await flushMicrotasks(); // combined.abort runs, signal aborts, 'combined fired' scheduled
  await flushMicrotasks(); // 'combined fired' prints here
  print('combined aborted after: ${combined.signal.aborted}');
}

Future<void> demoTimeout() async {
  print('\n--- AbortController timeout ---');
  final controller = AbortController(timeout: Duration(milliseconds: 300));
  controller.signal.register(() => print('timed out'));

  print('aborted before wait: ${controller.signal.aborted}');
  await Future.delayed(Duration(milliseconds: 500));
  print('aborted after wait: ${controller.signal.aborted}');
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
  final controller = AbortController();
  final slowFuture = Future.delayed(Duration(milliseconds: 500), () => 99);
  Future.delayed(Duration(milliseconds: 100), controller.abort);
  try {
    await waitCancellable(slowFuture, controller.signal);
  } on AbortException {
    print('waitCancellable threw AbortException');
  }

  print('\n--- waitCancellable: already aborted on entry ---');
  final controller2 = AbortController()..abort();
  try {
    await waitCancellable(Future.value(1), controller2.signal);
  } on AbortException {
    print('waitCancellable threw AbortException immediately');
  }
}

Future<void> demoSleep() async {
  print('\n--- sleep: completes normally ---');
  await sleep(Duration(milliseconds: 100));
  print('sleep completed');

  print('\n--- sleep: aborted before completing ---');
  final controller = AbortController();
  Future.delayed(Duration(milliseconds: 50), controller.abort);
  try {
    await sleep(Duration(milliseconds: 500), controller.signal);
  } on AbortException {
    print('threw AbortException');
  }

  print('\n--- sleep: already aborted ---');
  final controller2 = AbortController()..abort();
  try {
    await sleep(Duration(milliseconds: 100), controller2.signal);
  } on AbortException {
    print('threw AbortException immediately');
  }
}

Future<void> demoWaitAll() async {
  print('\n--- waitAll: all succeed ---');
  final results = await TaskGroup.waitAll([
    (signal) async { await sleep(Duration(milliseconds: 100), signal); return 1; },
    (signal) async { await sleep(Duration(milliseconds: 50),  signal); return 2; },
    (signal) async { await sleep(Duration(milliseconds: 150), signal); return 3; },
  ]);
  print('results: $results');

  print('\n--- waitAll: one fails, others cancelled ---');
  try {
    await TaskGroup.waitAll(<Future<int> Function(AbortSignal)>[
      (signal) async { await sleep(Duration(milliseconds: 200), signal); return 1; },
      (signal) async {
        await sleep(Duration(milliseconds: 50), signal);
        throw Exception('task 2 failed');
      },
      (signal) async { await sleep(Duration(milliseconds: 200), signal); return 3; },
    ]);
  } on AggregateException catch (e) {
    print('AggregateException with ${e.exceptions.length} exception(s): '
        '${e.exceptions.first}');
  }

  print('\n--- waitAll: outer signal aborted ---');
  final controller = AbortController();
  Future.delayed(Duration(milliseconds: 50), controller.abort);
  try {
    await TaskGroup.waitAll(
      [
        (signal) async { await sleep(Duration(seconds: 10), signal); return 1; },
        (signal) async { await sleep(Duration(seconds: 10), signal); return 2; },
      ],
      parentSignal: controller.signal,
    );
  } on AbortException {
    print('threw AbortException');
  }

  print('\n--- waitAll: cleanUp called on successes when another fails ---');
  try {
    await TaskGroup.waitAll(<Future<String> Function(AbortSignal)>[
      (signal) async { await sleep(Duration(milliseconds: 50), signal); return 'resource-A'; },
      (signal) async {
        await sleep(Duration(milliseconds: 100), signal);
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
    final controller = AbortController()..abort();
    try {
      await connectSocket('127.0.0.1', server2.port,
          signal: controller.signal);
    } on AbortException {
      print('threw AbortException immediately');
    }
  } finally {
    await server2.close();
  }

  print('\n--- connectSocket: aborted during connect ---');
  // 10.0.0.1 is non-routable here so the connect hangs, letting abort win
  final signal = AbortController(timeout: Duration(milliseconds: 100)).signal;
  try {
    await connectSocket('10.0.0.1', 80, signal: signal);
  } on AbortException {
    print('threw AbortException mid-connect');
  }
}

Future<void> demoTaskGroup() async {
  print('\n--- TaskGroup.using: all tasks succeed ---');
  final order = <int>[];
  await TaskGroup.using((tg) async {
    tg.spawn((signal) async {
      await sleep(Duration(milliseconds: 100), signal);
      order.add(1);
    });
    tg.spawn((signal) async {
      await sleep(Duration(milliseconds: 50), signal);
      order.add(2);
    });
  });
  print('completed, finish order: $order'); // expect [2, 1]

  print('\n--- TaskGroup.using: one task fails, sibling cancelled ---');
  try {
    await TaskGroup.using((tg) async {
      tg.spawn((signal) async {
        await sleep(Duration(milliseconds: 200), signal);
        order.add(99); // should not reach here
      });
      tg.spawn((signal) async {
        await sleep(Duration(milliseconds: 50), signal);
        throw Exception('task failed');
      });
    });
  } on AggregateException catch (e) {
    print('AggregateException: ${e.exceptions}');
    print('sibling was cancelled (99 not in order): ${!order.contains(99)}');
  }

  print('\n--- TaskGroup.using: body itself throws ---');
  try {
    await TaskGroup.using((tg) async {
      tg.spawn((signal) async {
        await sleep(Duration(milliseconds: 200), signal);
      });
      throw Exception('body failed');
    });
  } on AggregateException catch (e) {
    print('AggregateException from body: ${e.exceptions}');
  }

  print('\n--- TaskGroup: spawnWithFuture returns individual result ---');
  final tg = TaskGroup();
  final fut = tg.spawnWithFuture((signal) async {
    await sleep(Duration(milliseconds: 50), signal);
    return 42;
  });
  await tg.waitComplete();
  print('individual result: ${await fut}');
  print('completed: ${tg.completed}');

  print('\n--- TaskGroup: waitComplete() called twice returns same future ---');
  final tg2 = TaskGroup();
  tg2.spawn((signal) async => sleep(Duration(milliseconds: 50), signal));
  final f1 = tg2.waitComplete();
  final f2 = tg2.waitComplete();
  await f1;
  print('same future: ${identical(f1, f2)}');

  print('\n--- TaskGroup: abort() with no task errors completes normally ---');
  final tg3 = TaskGroup();
  tg3.spawn((signal) async {
    await sleep(Duration(seconds: 10), signal);
  });
  Future.delayed(Duration(milliseconds: 50), tg3.abort);
  await tg3.waitComplete();
  print('completed normally, signal aborted: ${tg3.signal.aborted}');

  print('\n--- TaskGroup: spawn() after completed throws StateError ---');
  final tg4 = TaskGroup();
  await tg4.waitComplete();
  try {
    tg4.spawn((_) async {});
  } on StateError catch (e) {
    print('StateError: $e');
  }

  print('\n--- TaskGroup: abort() after completed throws StateError ---');
  final tg5 = TaskGroup();
  await tg5.waitComplete();
  try {
    tg5.abort();
  } on StateError catch (e) {
    print('StateError: $e');
  }

  print('\n--- TaskGroup: parentSignal aborts the group ---');
  final controller = AbortController();
  Future.delayed(Duration(milliseconds: 50), controller.abort);
  try {
    await TaskGroup.using((tg) async {
      tg.spawn((signal) async {
        await sleep(Duration(seconds: 10), signal);
      });
    }, parentSignal: controller.signal);
  } on AbortException {
    print('threw AbortException from parentSignal');
  }

  print('\n--- TaskGroup: timeout fires, raiseOnTimeout=true ---');
  try {
    await TaskGroup.using((tg) async {
      tg.spawn((signal) async {
        await sleep(Duration(seconds: 10), signal);
      });
    }, timeout: Duration(milliseconds: 50));
  } on TimeoutException {
    print('threw TimeoutException');
  }

  print('\n--- TaskGroup: timeout fires, raiseOnTimeout=false ---');
  final tg6 = TaskGroup(timeout: Duration(milliseconds: 50), raiseOnTimeout: false);
  tg6.spawn((signal) async {
    await sleep(Duration(seconds: 10), signal);
  });
  await tg6.waitComplete();
  print('completed normally, didTimeout: ${tg6.didTimeout}');
}

Future<void> demoStreamCancellable() async {
  print('\n--- streamCancellable: aborted mid-stream ---');
  final controller = AbortController();

  // Stream that emits 1, 2, 3 with 200ms gaps
  final source = Stream.periodic(Duration(milliseconds: 200), (i) => i + 1)
      .take(5);

  // Abort after 450ms — should receive 1 and 2, then AbortException
  Future.delayed(Duration(milliseconds: 450), controller.abort);

  try {
    await for (final value in streamCancellable(source, controller.signal)) {
      print('received: $value');
    }
  } on AbortException {
    print('stream threw AbortException');
  }

  print('\n--- streamCancellable: already aborted on entry ---');
  final controller2 = AbortController()..abort();
  try {
    await for (final _ in streamCancellable(Stream.value(1), controller2.signal)) {}
  } on AbortException {
    print('stream threw AbortException immediately');
  }

  print('\n--- streamCancellable: source completes before abort ---');
  final controller3 = AbortController();
  final values = <int>[];
  await for (final v in streamCancellable(
    Stream.fromIterable([10, 20, 30]),
    controller3.signal,
  )) {
    values.add(v);
  }
  print('completed normally, values: $values');
}
