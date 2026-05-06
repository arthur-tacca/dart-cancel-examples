import 'dart:io';

import 'package:dart_cancel_examples/abort.dart';
import 'package:dart_cancel_examples/cancellable.dart';
import 'package:dart_cancel_examples/networking.dart';
import 'package:dart_cancel_examples/task_group.dart';

// --- Happy Eyeballs (RFC 8305) ---

Future<Socket> happyEyeballsConnect(
  String host,
  int port, {
  AbortSignal? signal,
  Duration stagger = const Duration(milliseconds: 250),
}) async {
  signal?.throwIfAborted();

  final addresses = await InternetAddress.lookup(host);
  if (addresses.isEmpty) {
    throw SocketException('No addresses resolved for $host');
  }

  Socket? winner;
  final errors = <Object>[];

  await TaskGroup.using(parentSignal: signal, body: (tg) async {
    for (var i = 0; i < addresses.length; i++) {
      final address = addresses[i];
      final delay = stagger * i;
      tg.spawn((sig) async {
        try {
          if (delay > Duration.zero) {
            await sleep(delay, sig);
          }
          final socket = await connectSocket(address.address, port, signal: sig);
          if (winner == null) {
            winner = socket;
            tg.abort();   // I won — cancel siblings
          } else {
            socket.destroy();   // Lost the race
          }
        } on AbortException {
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
  required Future<void> Function(Socket, AbortSignal) handleConnection,
  required Future<void> Function(AbortSignal) waitForShutdownSignal,
  required Duration shutdownGrace,
}) async {
  await TaskGroup.using(body: (connectionTg) async {
    await TaskGroup.using(parentSignal: connectionTg.signal, body: (listenerTg) async {
      for (final port in ports) {
        listenerTg.spawn((signal) async {
          final server = await ServerSocket.bind('0.0.0.0', port);
          try {
            await for (final socket in streamCancellable(server, signal)) {
              connectionTg.spawn((signal) async {
                try {
                  await handleConnection(socket, signal);
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
      await waitForShutdownSignal(listenerTg.signal);
      listenerTg.abort();
    });
    connectionTg.setTimeout(shutdownGrace);
  });
}

Future<void> runServe() async {
  await serve(
    [8080],
    handleConnection: (socket, signal) async {
      await for (final data in streamCancellable(socket, signal)) {
        socket.add(data);
      }
    },
    waitForShutdownSignal: (signal) => sleep(Duration(seconds: 30), signal),
    shutdownGrace: Duration(seconds: 5),
  );
}

void main() async {
  await runHappyEyeballs();
}
