import 'dart:async';
import 'dart:io';
import 'package:dart_cancel_examples/abort.dart';
import 'package:dart_cancel_examples/utils.dart';

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
  await demoWaitAllAlt();
  await demoConnectSocket();
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
  print('\n--- AbortSignal.any() ---');
  final c1 = AbortController();
  final c2 = AbortController();
  final combined = AbortSignal.any([c1.signal, c2.signal]);

  combined.register(() => print('combined fired'));

  print('combined aborted before: ${combined.aborted}');
  c2.abort(); // schedules onAbort microtask
  await flushMicrotasks(); // onAbort runs, combined aborts, 'combined fired' scheduled
  await flushMicrotasks(); // 'combined fired' prints here
  print('combined aborted after: ${combined.aborted}');
}

Future<void> demoTimeout() async {
  print('\n--- AbortSignal.timeout() ---');
  final signal = AbortSignal.timeout(Duration(milliseconds: 300));
  signal.register(() => print('timed out'));

  print('aborted before wait: ${signal.aborted}');
  await Future.delayed(Duration(milliseconds: 500));
  print('aborted after wait: ${signal.aborted}');
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
  final results = await waitAll([
    (signal) async { await sleep(Duration(milliseconds: 100), signal); return 1; },
    (signal) async { await sleep(Duration(milliseconds: 50),  signal); return 2; },
    (signal) async { await sleep(Duration(milliseconds: 150), signal); return 3; },
  ]);
  print('results: $results');

  print('\n--- waitAll: one fails, others cancelled ---');
  try {
    await waitAll(<Future<int> Function(AbortSignal)>[
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
    await waitAll(
      [
        (signal) async { await sleep(Duration(seconds: 10), signal); return 1; },
        (signal) async { await sleep(Duration(seconds: 10), signal); return 2; },
      ],
      signal: controller.signal,
    );
  } on AbortException {
    print('threw AbortException');
  }

  print('\n--- waitAll: cleanUp called on successes when another fails ---');
  try {
    await waitAll(<Future<String> Function(AbortSignal)>[
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

Future<void> demoWaitAllAlt() async {
  print('\n--- waitAllAlt: one fails, results inspected in catch ---');
  final results = <String, Outcome<int>>{};
  try {
    await waitAllAlt(<String, Future<int> Function(AbortSignal)>{
      'task 1': (signal) async { await sleep(Duration(milliseconds: 50),  signal); return 1; },
      'task 2': (signal) async {
        await sleep(Duration(milliseconds: 100), signal);
        throw Exception('task 2 failed');
      },
      'task 3': (signal) async { await sleep(Duration(milliseconds: 50),  signal); return 3; },
    }, results: results);
  } on AggregateException {
    for (final entry in results.entries) {
      final r = entry.value;
      print('${entry.key}: '
          '${r.success ? 'ok (${r.get()})' : 'failed (${r.exception})'}');
    }
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
          abortSignal: controller.signal);
    } on AbortException {
      print('threw AbortException immediately');
    }
  } finally {
    await server2.close();
  }

  print('\n--- connectSocket: aborted during connect ---');
  // 10.0.0.1 is non-routable here so the connect hangs, letting abort win
  final signal = AbortSignal.timeout(Duration(milliseconds: 100));
  try {
    await connectSocket('10.0.0.1', 80, abortSignal: signal);
  } on AbortException {
    print('threw AbortException mid-connect');
  }
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
