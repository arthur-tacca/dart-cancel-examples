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

  Socket? winner;
  final errors = <Object>[];

  final tg = TaskGroup(parentCancelToken: cancelToken);
  await tg.using((_) async {
    for (var i = 0; i < addresses.length; i++) {
      final address = addresses[i];
      final delay = stagger * i;
      tg.spawn((cancelToken) async {
        try {
          if (delay > Duration.zero) {
            await sleep(delay, cancelToken);
          }
          final socket = await connectSocket(address.address, port, cancelToken: cancelToken);
          if (winner == null) {
            winner = socket;
            tg.scope.cancel();   // I won — cancel siblings
          } else {
            socket.destroy();   // Lost the race
          }
        } on CancelException {
          rethrow;
        } catch (e) {
          errors.add(e);   // Let siblings keep racing
        }
      });
    }
  });

  if (winner == null) {
    throw SocketException(
      'Could not connect to $host:$port (${errors.length} attempt(s) failed)',
    );
  }
  return winner!;
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
