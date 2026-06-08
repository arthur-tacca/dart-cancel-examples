import 'dart:io';

import 'package:dart_cancel_examples/cancel_core.dart';

/// Connects a [Socket] to [host]:[port].
///
/// Reads [currentCancelToken] at call time. If it's already cancelled, throws
/// [CancelException] immediately. If it cancels while the connection is in
/// progress, the attempt is cancelled and [CancelException] is thrown. A
/// genuine connection error (e.g. refused or unreachable) throws
/// [SocketException] as normal.
Future<Socket> connectSocket(String host, int port) async {
  final token = currentCancelToken;
  token?.throwIfCancelled();
  final task = await Socket.startConnect(host, port);
  final registration = token?.register(task.cancel);
  try {
    return await task.socket;
  } on SocketException {
    if (token?.cancelled ?? false) {
      throw const CancelException();
    }
    rethrow;
  } finally {
    registration?.unregister();
  }
}

/// Connects a [SecureSocket] to [host]:[port].
///
/// Reads [currentCancelToken] at call time. If it's already cancelled, throws
/// [CancelException] immediately. If it cancels while the connection is in
/// progress, the attempt is cancelled and [CancelException] is thrown. A
/// genuine connection error (e.g. refused or unreachable) throws
/// [SocketException] as normal.
Future<SecureSocket> connectSecureSocket(
  String host,
  int port, {
  SecurityContext? context,
  bool Function(X509Certificate)? onBadCertificate,
}) async {
  final token = currentCancelToken;
  token?.throwIfCancelled();
  final task = await SecureSocket.startConnect(
    host,
    port,
    context: context,
    onBadCertificate: onBadCertificate,
  );
  final registration = token?.register(task.cancel);
  try {
    return await task.socket;
  } on SocketException {
    if (token?.cancelled ?? false) {
      throw const CancelException();
    }
    rethrow;
  } finally {
    registration?.unregister();
  }
}
