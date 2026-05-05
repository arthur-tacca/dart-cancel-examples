import 'dart:io';

import 'package:dart_cancel_examples/abort.dart';

/// Connects a [Socket] to [host]:[port], with optional cancellation.
///
/// If [signal] is already aborted when called, throws [AbortException]
/// immediately. If it is aborted while the connection is in progress, the
/// attempt is cancelled and [AbortException] is thrown. A genuine connection
/// error (e.g. refused or unreachable) throws [SocketException] as normal.
Future<Socket> connectSocket(
  String host,
  int port, {
  AbortSignal? signal,
}) async {
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

/// Connects a [SecureSocket] to [host]:[port], with optional cancellation.
///
/// If [signal] is already aborted when called, throws [AbortException]
/// immediately. If it is aborted while the connection is in progress, the
/// attempt is cancelled and [AbortException] is thrown. A genuine connection
/// error (e.g. refused or unreachable) throws [SocketException] as normal.
Future<SecureSocket> connectSecureSocket(
  String host,
  int port, {
  SecurityContext? context,
  bool Function(X509Certificate)? onBadCertificate,
  AbortSignal? signal,
}) async {
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
