import 'dart:io';

import 'package:dart_cancel_examples/abort.dart';

/// Connects a [Socket] to [host]:[port].
///
/// Reads [currentSignal] at call time. If it's already aborted, throws
/// [AbortException] immediately. If it aborts while the connection is in
/// progress, the attempt is cancelled and [AbortException] is thrown. A
/// genuine connection error (e.g. refused or unreachable) throws
/// [SocketException] as normal.
Future<Socket> connectSocket(String host, int port) async {
  final signal = currentSignal;
  signal?.throwIfAborted();
  final task = await Socket.startConnect(host, port);
  final registration = signal?.register(task.cancel);
  try {
    return await task.socket;
  } on SocketException {
    if (signal?.aborted ?? false) {
      throw const AbortException();
    }
    rethrow;
  } finally {
    registration?.unregister();
  }
}

/// Connects a [SecureSocket] to [host]:[port].
///
/// Reads [currentSignal] at call time. If it's already aborted, throws
/// [AbortException] immediately. If it aborts while the connection is in
/// progress, the attempt is cancelled and [AbortException] is thrown. A
/// genuine connection error (e.g. refused or unreachable) throws
/// [SocketException] as normal.
Future<SecureSocket> connectSecureSocket(
  String host,
  int port, {
  SecurityContext? context,
  bool Function(X509Certificate)? onBadCertificate,
}) async {
  final signal = currentSignal;
  signal?.throwIfAborted();
  final task = await SecureSocket.startConnect(
    host,
    port,
    context: context,
    onBadCertificate: onBadCertificate,
  );
  final registration = signal?.register(task.cancel);
  try {
    return await task.socket;
  } on SocketException {
    if (signal?.aborted ?? false) {
      throw const AbortException();
    }
    rethrow;
  } finally {
    registration?.unregister();
  }
}
