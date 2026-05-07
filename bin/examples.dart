import 'dart:io';
import 'dart:typed_data';

import 'package:dart_cancel_examples/abort.dart';
import 'package:dart_cancel_examples/cancellable.dart';
import 'package:dart_cancel_examples/networking.dart';
import 'package:dart_cancel_examples/task_group.dart';

// --- readBytes: cancellable read of N bytes from a TCP server ---
//
// No signal parameter — cancellation is picked up from the ambient zone, so
// callers either run this inside a TaskGroup body or fork a zone with a
// signal themselves.

Future<Uint8List> readBytes(String host, int port, int byteCount) async {
  final socket = await connectSocket(host, port);
  try {
    final buffer = BytesBuilder(copy: false);
    await for (final chunk in streamCancellable(socket)) {
      buffer.add(chunk);
      if (buffer.length >= byteCount) break;
    }
    if (buffer.length < byteCount) {
      throw SocketException('Connection closed before $byteCount bytes received');
    }
    return buffer.takeBytes();
  } finally {
    socket.destroy();
  }
}

// --- TaskGroup: basic spawn + waitComplete ---

Future<Map<String, Uint8List>> remoteReads({
  required ({String host, int port}) alpha,
  required ({String host, int port}) beta,
}) async {
  final results = <String, Uint8List>{};
  TaskGroup taskGroup = TaskGroup();
  taskGroup.spawn(() async {
    results['alpha'] = await readBytes(alpha.host, alpha.port, 100);
  });
  taskGroup.spawn(() async {
    results['beta'] = await readBytes(beta.host, beta.port, 100);
  });
  await taskGroup.waitComplete();
  return results;
}

Future<void> runRemoteReads() async {
  print('--- remoteReads (TaskGroup + spawn + waitComplete) ---');
  final alphaServer = await _startBytesServer(100);
  final betaServer = await _startBytesServer(100);
  try {
    final results = await remoteReads(
      alpha: (host: '127.0.0.1', port: alphaServer.port),
      beta: (host: '127.0.0.1', port: betaServer.port),
    );
    print('alpha: ${results['alpha']!.length} bytes, '
        'beta: ${results['beta']!.length} bytes');
  } finally {
    await alphaServer.close();
    await betaServer.close();
  }
}

// --- TaskGroup.using: scoped spawn ---

Future<Map<String, Uint8List>> remoteReadsUsing({
  required ({String host, int port}) alpha,
  required ({String host, int port}) beta,
}) async {
  final results = <String, Uint8List>{};
  await TaskGroup.using(body: (taskGroup) async {
    taskGroup.spawn(() async {
      results['alpha'] = await readBytes(alpha.host, alpha.port, 100);
    });
    taskGroup.spawn(() async {
      results['beta'] = await readBytes(beta.host, beta.port, 100);
    });
  });
  return results;
}

Future<void> runRemoteReadsUsing() async {
  print('\n--- remoteReadsUsing (TaskGroup.using) ---');
  final alphaServer = await _startBytesServer(100);
  final betaServer = await _startBytesServer(100);
  try {
    final results = await remoteReadsUsing(
      alpha: (host: '127.0.0.1', port: alphaServer.port),
      beta: (host: '127.0.0.1', port: betaServer.port),
    );
    print('alpha: ${results['alpha']!.length} bytes, '
        'beta: ${results['beta']!.length} bytes');
  } finally {
    await alphaServer.close();
    await betaServer.close();
  }
}

/// Local server used by the run* scaffolds: sends [byteCount] bytes to each
/// connection, then closes.
Future<ServerSocket> _startBytesServer(int byteCount) async {
  final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  server.listen((socket) async {
    socket.add(List.filled(byteCount, 0x41));
    await socket.flush();
    socket.destroy();
  });
  return server;
}

// --- Happy Eyeballs (RFC 8305) ---
//
// No signal parameter — cancellation is picked up from the ambient zone
// (TaskGroup.using inherits via currentSignal). Callers wire external
// cancellation by aborting the enclosing TaskGroup.

Future<Socket> happyEyeballsConnect(
  String host,
  int port, {
  Duration stagger = const Duration(milliseconds: 250),
}) async {
  // Workaround: InternetAddress.lookup is not cancellable, so we yield
  // through a zero-duration sleep first as a cancellation point — if the
  // ambient signal is already aborted, this throws AbortException before
  // we waste a DNS lookup. If/when Dart's DNS resolver becomes
  // cancellation-aware this line can be removed; the rest of the function
  // aborts cleanly on its own through the inner TaskGroup.
  await sleep(Duration.zero);

  final addresses = await InternetAddress.lookup(host);
  if (addresses.isEmpty) {
    throw SocketException('No addresses resolved for $host');
  }

  Socket? winner;
  final errors = <Object>[];

  await TaskGroup.using(body: (tg) async {
    for (var i = 0; i < addresses.length; i++) {
      final address = addresses[i];
      final delay = stagger * i;
      tg.spawn(() async {
        try {
          if (delay > Duration.zero) {
            await sleep(delay);
          }
          final socket = await connectSocket(address.address, port);
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
  print('\n--- happyEyeballsConnect ---');
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
// Both user-supplied callbacks lose their AbortSignal arg — they read from
// ambient. The inner TaskGroup.using doesn't pass parentSignal: it inherits
// from the outer group's zone automatically.

Future<void> serve(
  List<int> ports, {
  required Future<void> Function(Socket) handleConnection,
  required Future<void> Function() waitForShutdownSignal,
  required Duration shutdownGrace,
}) async {
  await TaskGroup.using(body: (connectionTg) async {
    await TaskGroup.using(body: (listenerTg) async {
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
      listenerTg.abort();
    });
    connectionTg.setTimeout(shutdownGrace);
  });
}

Future<void> runServe() async {
  print('\n--- serve (1s lifetime) ---');
  await serve(
    [8080],
    handleConnection: (socket) async {
      await for (final data in streamCancellable(socket)) {
        socket.add(data);
      }
    },
    waitForShutdownSignal: () => sleep(Duration(seconds: 1)),
    shutdownGrace: Duration(milliseconds: 500),
  );
  print('serve: shut down cleanly');
}

void main() async {
  await runRemoteReads();
  await runRemoteReadsUsing();
  await runHappyEyeballs();
  await runServe();
}
