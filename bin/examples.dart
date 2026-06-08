import 'dart:collection';
import 'dart:io';

import 'package:dart_cancel_examples/cancel_core.dart';
import 'package:dart_cancel_examples/cancellable.dart';
import 'package:dart_cancel_examples/networking.dart';
import 'package:dart_cancel_examples/task_group.dart';

// --- Happy Eyeballs (RFC 8305) ---
//
// No token parameter — cancellation is picked up from the ambient zone
// (set by the enclosing TaskGroup). Callers wire external cancellation by
// canceling the enclosing scope or task group.

Future<Socket> happyEyeballsConnect(
  String host,
  int port, {
  Duration stagger = const Duration(milliseconds: 250),
}) async {
  // Workaround: InternetAddress.lookup is not cancellable, so we yield
  // through a zero-duration sleep first as a cancellation point — if the
  // ambient token is already cancelled, this throws CancelException before
  // we waste a DNS lookup. The rest of the function cancels cleanly through
  // the inner TaskGroup.
  await sleep(Duration.zero);

  final addresses = await InternetAddress.lookup(host);
  if (addresses.isEmpty) {
    throw SocketException('No addresses resolved for $host');
  }

  final winners = Queue<Socket>();
  final errors = <Object>[];

  final tg = TaskGroup();
  try {
    await tg.using(() async {
      Future<Outcome<void>>? previous;
      for (var i = 0; i < addresses.length; i++) {
        if (previous != null) {
          // Wait until the previously-spawned attempt finishes, capped at
          // `stagger`. The gate inherits the group's ambient token, so its
          // timeout provides the cap while waitCancellable lets the attempt
          // finishing early (a fast failure or a connect) cut the wait short.
          await CancelScope(timeout: stagger).using(() async {
            await waitCancellable(previous!);
          });
          // If another attempt won (cancelling the group) during the wait,
          // stop spawning the rest. sleep(0) throws on the ambient group token.
          await sleep(Duration.zero);
        }
        final address = addresses[i];
        previous = tg.spawnWithFuture(() async {
          try {
            final socket = await connectSocket(address.address, port);
            winners.add(socket);   // connected — record it...
            tg.scope.cancel();     // ...and cancel the stragglers
          } on SocketException catch (e) {
            errors.add(e);   // Connection failed — let siblings keep racing
          }
        });
      }
    });

    if (winners.isEmpty) {
      throw SocketException(
        'Could not connect to $host:$port (${errors.length} attempt(s) failed)',
      );
    }
    // First to connect is the race winner. Remove it so the finally below
    // doesn't destroy it; anything still queued is a straggler.
    return winners.removeFirst();
  } finally {
    // Reached on every exit. Success: stragglers that also connected. Throw
    // (parent cancellation, or no winner): every socket that connected. None
    // of these are being returned, so close them.
    for (final socket in winners) {
      socket.destroy();
    }
  }
}

Future<void> runHappyEyeballs() async {
  try {
    final socket = await happyEyeballsConnect('example.com', 80);
    print('Connected to ${socket.remoteAddress.address}:${socket.remotePort}');
    socket.destroy();
  } on SocketException catch (e) {
    print('Connection failed: $e');
  }
}

// --- TCP server with graceful shutdown ---
//
// User-supplied callbacks drop their CancelToken arg — they read from
// ambient. The inner TaskGroup inherits the outer group's token as parent
// automatically (via the zone), so no parentToken threading is needed.

Future<void> serve(
  List<int> ports, {
  required Future<void> Function(Socket) handleConnection,
  required Future<void> Function() waitForShutdownSignal,
  required Duration shutdownGrace,
}) async {
  final connectionTg = TaskGroup();
  await connectionTg.using(() async {
    final listenerTg = TaskGroup();
    await listenerTg.using(() async {
      for (final port in ports) {
        listenerTg.spawn(() async {
          final server = await ServerSocket.bind('0.0.0.0', port);
          try {
            await for (final socket in streamCancellable(server)) {
              connectionTg.spawn(() async {
                try {
                  await handleConnection(socket);
                } finally {
                  socket.destroy();
                }
              });
            }
          } finally {
            await server.close();
          }
        });
      }
      await waitForShutdownSignal();
      listenerTg.scope.cancel();
    });
    connectionTg.scope.setTimeout(shutdownGrace);
  });
}

Future<void> runServe() async {
  await serve(
    [8080],
    handleConnection: (socket) async {
      await for (final data in streamCancellable(socket)) {
        socket.add(data);
      }
    },
    waitForShutdownSignal: () => sleep(Duration(seconds: 30)),
    shutdownGrace: Duration(seconds: 5),
  );
}

void main() async {
  await runHappyEyeballs();
}
