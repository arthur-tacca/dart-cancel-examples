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
  final scope = CancelScope();
  await scope.using((cancelToken) async {
    cancelToken.register(() => print('cancelled!'));

    print('cancelled before: ${cancelToken.cancelled}');
    scope.cancel();
    print('cancelled after: ${cancelToken.cancelled}'); // true immediately
    await flushMicrotasks(); // 'cancelled!' prints here

    try {
      cancelToken.throwIfCancelled();
    } on CancelException {
      print('throwIfCancelled() threw CancelException');
    }
  });
}

Future<void> demoRegisterUnregister() async {
  print('\n--- register / unregister before cancel ---');
  final scope1 = CancelScope();
  await scope1.using((cancelToken) async {
    final reg = cancelToken.register(() => print('this should not print'));
    reg.unregister();
    scope1.cancel(); // callback was removed before cancel, nothing fires
    await flushMicrotasks();

    try {
      reg.unregister(); // second call — should throw StateError
    } on StateError catch (e) {
      print('double unregister caught: $e');
    }
  });

  print('\n--- unregister after cancel, before microtask fires ---');
  final scope2 = CancelScope();
  await scope2.using((cancelToken) async {
    final reg = cancelToken.register(() => print('this should not print either'));
    scope2.cancel();             // schedules microtask
    reg.unregister();           // unregistered before microtask fires
    await flushMicrotasks();    // microtask checks list != null, skips callback
    print('callback was suppressed');
  });
}

Future<void> demoTimeout() async {
  print('\n--- CancelScope timeout ---');
  final scope = CancelScope(timeout: Duration(milliseconds: 300));
  await scope.using((cancelToken) async {
    cancelToken.register(() => print('timed out'));
    print('cancelled before wait: ${cancelToken.cancelled}');
    // Future.delayed doesn't observe the token, so the body completes normally
    // and the scope absorbs the timeout silently.
    await Future.delayed(Duration(milliseconds: 500));
    print('cancelled after wait: ${cancelToken.cancelled}');
  });
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
  final scope = CancelScope();
  final slowFuture = Future.delayed(Duration(milliseconds: 500), () => 99);
  Future.delayed(Duration(milliseconds: 100), scope.cancel);
  await scope.using((cancelToken) async {
    try {
      await waitCancellable(slowFuture, cancelToken);
    } on CancelException {
      print('waitCancellable threw CancelException');
    }
  });

  print('\n--- waitCancellable: already cancelled on entry ---');
  final scope2 = CancelScope()..cancel();
  await scope2.using((cancelToken) async {
    try {
      await waitCancellable(Future.value(1), cancelToken);
    } on CancelException {
      print('waitCancellable threw CancelException immediately');
    }
  });
}

Future<void> demoSleep() async {
  print('\n--- sleep: completes normally ---');
  await sleep(Duration(milliseconds: 100));
  print('sleep completed');

  print('\n--- sleep: cancelled before completing ---');
  final scope = CancelScope();
  Future.delayed(Duration(milliseconds: 50), scope.cancel);
  await scope.using((cancelToken) async {
    try {
      await sleep(Duration(milliseconds: 500), cancelToken);
    } on CancelException {
      print('threw CancelException');
    }
  });

  print('\n--- sleep: already cancelled ---');
  final scope2 = CancelScope()..cancel();
  await scope2.using((cancelToken) async {
    try {
      await sleep(Duration(milliseconds: 100), cancelToken);
    } on CancelException {
      print('threw CancelException immediately');
    }
  });
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
  final scope = CancelScope();
  Future.delayed(Duration(milliseconds: 50), scope.cancel);
  await scope.using((cancelToken) async {
    try {
      await TaskGroup.waitAll(
        [
          (cancelToken) async { await sleep(Duration(seconds: 10), cancelToken); return 1; },
          (cancelToken) async { await sleep(Duration(seconds: 10), cancelToken); return 2; },
        ],
        parentCancelToken: cancelToken,
      );
    } on CancelException {
      print('threw CancelException');
    }
  });

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
  final scope = CancelScope();
  Future.delayed(Duration(milliseconds: 30), scope.cancel);
  await scope.using((cancelToken) async {
    try {
      await TaskGroup.waitAny(
        [
          (cancelToken) async { await sleep(Duration(seconds: 10), cancelToken); return 1; },
          (cancelToken) async { await sleep(Duration(seconds: 10), cancelToken); return 2; },
        ],
        parentCancelToken: cancelToken,
      );
    } on CancelException {
      print('threw CancelException');
    }
  });

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
    final scope = CancelScope()..cancel();
    await scope.using((cancelToken) async {
      try {
        await connectSocket('127.0.0.1', server2.port, cancelToken: cancelToken);
      } on CancelException {
        print('threw CancelException immediately');
      }
    });
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
  final scope = CancelScope(timeout: Duration(milliseconds: 100));
  await scope.using((cancelToken) async {
    try {
      final socket = await connectSocket('198.51.100.1', 81, cancelToken: cancelToken);
      socket.destroy();
      print('unexpectedly connected; socket destroyed');
    } on CancelException {
      print('threw CancelException mid-connect');
    }
  });
}

Future<void> demoTaskGroup() async {
  print('\n--- TaskGroup.using: all tasks succeed ---');
  final order = <int>[];
  final tgA = TaskGroup();
  await tgA.using((_) async {
    tgA.spawn((cancelToken) async {
      await sleep(Duration(milliseconds: 100), cancelToken);
      order.add(1);
    });
    tgA.spawn((cancelToken) async {
      await sleep(Duration(milliseconds: 50), cancelToken);
      order.add(2);
    });
  });
  print('completed, finish order: $order'); // expect [2, 1]

  print('\n--- TaskGroup.using: one task fails, sibling cancelled ---');
  try {
    final tgB = TaskGroup();
    await tgB.using((_) async {
      tgB.spawn((cancelToken) async {
        await sleep(Duration(milliseconds: 200), cancelToken);
        order.add(99); // should not reach here
      });
      tgB.spawn((cancelToken) async {
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
    final tgC = TaskGroup();
    await tgC.using((_) async {
      tgC.spawn((cancelToken) async {
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
  Future.delayed(Duration(milliseconds: 50), tg3.scope.cancel);
  await tg3.waitComplete();
  print('completed normally, cancelCaught: ${tg3.scope.cancelCaught}');

  print('\n--- TaskGroup: spawn() after completed throws StateError ---');
  final tg4 = TaskGroup();
  await tg4.waitComplete();
  try {
    tg4.spawn((_) async {});
  } on StateError catch (e) {
    print('StateError: $e');
  }

  print('\n--- TaskGroup: scope.cancel() after completed does not throw ---');
  final tg5 = TaskGroup();
  await tg5.waitComplete();
  tg5.scope.cancel();
  print('no error thrown');

  print('\n--- TaskGroup: parentCancelToken cancels the group ---');
  final scope = CancelScope();
  Future.delayed(Duration(milliseconds: 50), scope.cancel);
  await scope.using((cancelToken) async {
    try {
      final tgD = TaskGroup(parentCancelToken: cancelToken);
      await tgD.using((_) async {
        tgD.spawn((cancelToken) async {
          await sleep(Duration(seconds: 10), cancelToken);
        });
      });
    } on CancelException {
      print('threw CancelException from parentCancelToken');
    }
  });

  print('\n--- TaskGroup: scope timeout fires, group completes normally ---');
  final tgE = TaskGroup();
  tgE.scope.setTimeout(Duration(milliseconds: 50));
  tgE.spawn((cancelToken) async {
    await sleep(Duration(seconds: 10), cancelToken);
  });
  await tgE.waitComplete();
  print('completed normally, cancelCaught: ${tgE.scope.cancelCaught}');

  print('\n--- TaskGroup: setTimeout() mid-flight cancels the group ---');
  final tgF = TaskGroup();
  await tgF.using((_) async {
    tgF.spawn((cancelToken) async {
      await sleep(Duration(seconds: 10), cancelToken);
    });
    await sleep(Duration(milliseconds: 50));
    // Set a short timeout after some initial work
    tgF.scope.setTimeout(Duration(milliseconds: 50));
  });
  print('completed normally, cancelCaught: ${tgF.scope.cancelCaught}');

  print('\n--- TaskGroup: setTimeout() after completed is a no-op ---');
  final tg7 = TaskGroup();
  await tg7.waitComplete();
  tg7.scope.setTimeout(Duration(milliseconds: 1));
  await sleep(Duration(milliseconds: 10));
  print('no error thrown');
}

Future<void> demoStreamCancellable() async {
  print('\n--- streamCancellable: cancelled mid-stream ---');
  final scope = CancelScope();

  // Stream that emits 1, 2, 3 with 200ms gaps
  final source = Stream.periodic(Duration(milliseconds: 200), (i) => i + 1)
      .take(5);

  // Cancel after 450ms — should receive 1 and 2, then CancelException
  Future.delayed(Duration(milliseconds: 450), scope.cancel);

  await scope.using((cancelToken) async {
    try {
      await for (final value in streamCancellable(source, cancelToken)) {
        print('received: $value');
      }
    } on CancelException {
      print('stream threw CancelException');
    }
  });

  print('\n--- streamCancellable: already cancelled on entry ---');
  final scope2 = CancelScope()..cancel();
  await scope2.using((cancelToken) async {
    try {
      await for (final _ in streamCancellable(Stream.value(1), cancelToken)) {}
    } on CancelException {
      print('stream threw CancelException immediately');
    }
  });

  print('\n--- streamCancellable: source completes before cancel ---');
  final scope3 = CancelScope();
  await scope3.using((cancelToken) async {
    final values = <int>[];
    await for (final v in streamCancellable(
      Stream.fromIterable([10, 20, 30]),
      cancelToken,
    )) {
      values.add(v);
    }
    print('completed normally, values: $values');
  });
}
