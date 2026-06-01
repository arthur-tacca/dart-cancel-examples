import 'dart:collection';
import 'dart:io';

import 'package:dart_cancel_examples/cancel_core.dart';
import 'package:dart_cancel_examples/cancellable.dart';
import 'package:dart_cancel_examples/networking.dart';
import 'package:dart_cancel_examples/task_group.dart';

// --- Happy Eyeballs (RFC 8305) ---

Future<Socket> happyEyeballsConnect(
  String host,
  int port, {
  CancelToken? cancelToken,
  Duration stagger = const Duration(milliseconds: 250),
}) async {
  cancelToken?.throwIfCancelled();

  final addresses = await InternetAddress.lookup(host);
  if (addresses.isEmpty) {
    throw SocketException('No addresses resolved for $host');
  }

  final winners = Queue<Socket>();
  final errors = <Object>[];

  final tg = TaskGroup(parentCancelToken: cancelToken);
  try {
    await tg.using((groupToken) async {
      Future<Outcome<void>>? previous;
      for (var i = 0; i < addresses.length; i++) {
        if (previous != null) {
          // Wait until the previously-spawned attempt finishes, capped at
          // `stagger`. The gate is a child of the group token, so its timeout
          // provides the cap while waitCancellable lets the attempt finishing
          // early (a fast failure or a connect) cut the wait short.
          await CancelScope(parentCancelToken: groupToken, timeout: stagger).using((token) async {
            await waitCancellable(previous!, token);
          });
          // If another attempt won (cancelling the group) during the wait,
          // stop spawning the rest.
          groupToken.throwIfCancelled();
        }
        final address = addresses[i];
        previous = tg.spawnWithFuture((cancelToken) async {
          try {
            final socket = await connectSocket(address.address, port, cancelToken: cancelToken);
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

Future<void> serve(
  List<int> ports, {
  required Future<void> Function(Socket, CancelToken) handleConnection,
  required Future<void> Function(CancelToken) waitForShutdownSignal,
  required Duration shutdownGrace,
}) async {
  final connectionTg = TaskGroup();
  await connectionTg.using((connectionCancelToken) async {
    final listenerTg = TaskGroup(parentCancelToken: connectionCancelToken);
    await listenerTg.using((listenerCancelToken) async {
      for (final port in ports) {
        listenerTg.spawn((cancelToken) async {
          final server = await ServerSocket.bind('0.0.0.0', port);
          try {
            await for (final socket in streamCancellable(server, cancelToken)) {
              connectionTg.spawn((cancelToken) async {
                try {
                  await handleConnection(socket, cancelToken);
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
      await waitForShutdownSignal(listenerCancelToken);
      listenerTg.scope.cancel();
    });
    connectionTg.scope.setTimeout(shutdownGrace);
  });
}

Future<void> runServe() async {
  await serve(
    [8080],
    handleConnection: (socket, cancelToken) async {
      await for (final data in streamCancellable(socket, cancelToken)) {
        socket.add(data);
      }
    },
    waitForShutdownSignal: (cancelToken) => sleep(Duration(seconds: 30), cancelToken),
    shutdownGrace: Duration(seconds: 5),
  );
}

void main() async {
  await runHappyEyeballs();
}
