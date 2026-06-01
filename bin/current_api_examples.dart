// readBytes() implemented with Dart's *current* cancellation API
// (CancelableOperation / CancelableCompleter from package:async), as shown in
// section "4. Why not the current API?" of the proposal gist:
// https://gist.github.com/arthur-tacca/accbd333a6378619936e34d184b0d152
//
// This is the foil to the README's "Example usage" section, which implements
// the same readBytes() with the cancel-token API proposed in this repo. Both
// the original (buggy) version and the fixed version from the gist are included
// here so the two approaches can be compared directly.

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:async/async.dart';

/// Stand-in for an internal exception a caller might raise to its own callers.
class MyException implements Exception {
  final String message;
  MyException(this.message);
  @override
  String toString() => 'MyException: $message';
}

/// Stand-in for some code that might request cancellation at a later point.
void mightCallLater(void Function() action) {
  Timer(const Duration(milliseconds: 50), action);
}

// How a caller invokes a cancellable operation with the current API. Contrast
// with the proposed-API `readWithCancel()` in the repo README.
Future<Uint8List> readWithCancel() async {
  final op = readBytes('example.com', 8080, 100);
  mightCallLater(() => unawaited(op.cancel()));
  final data = await op.valueOrCancellation();
  if (!op.isCanceled) {
    return data!;
  } else {
    throw MyException('Operation was interrupted');
  }
}

// readBytes() from the README. Can you find the two subtle bugs?
CancelableOperation<Uint8List> readBytes(String host, int port, int byteCount) {
  ConnectionTask<Socket>? connectTask;
  StreamSubscription<Uint8List>? subscription;
  Completer<void>? readDone;

  final completer = CancelableCompleter<Uint8List>(
    onCancel: () {
      connectTask?.cancel();
      subscription?.cancel();
      readDone?.completeError(
        SocketException('Operation cancelled'),
      );
    },
  );

  () async {
    Socket? socket;
    try {
      connectTask = await Socket.startConnect(host, port);
      socket = await connectTask!.socket;

      final buffer = BytesBuilder(copy: false);
      readDone = Completer<void>();

      subscription = socket.listen(
        (chunk) {
          buffer.add(chunk);
          if (buffer.length >= byteCount) {
            subscription!.cancel();
            readDone!.complete();
          }
        },
        onError: (e, st) {
          readDone!.completeError(e, st);
        },
        onDone: () {
          readDone!.completeError(
            SocketException('Connection closed before $byteCount bytes received'),
          );
        },
        cancelOnError: true,
      );

      await readDone!.future;

      completer.complete(buffer.takeBytes());
    } catch (e, st) {
      completer.completeError(e, st);
    } finally {
      socket?.destroy();
    }
  }();

  return completer.operation;
}

// Bugs:
//
// 1. If cancelled after completed (even much later), onCancel calls
//    readDone?.completeError() on an already completed Completer. This isn't allowed so it
//    raises an exception, which propagates synchronously out to the caller of
//    CancelableOperation.cancel() (though I had to look at the source code of
//    CancelableOperation to figure that out!). The fix to this is to check
//    !readDone!.isCompleted() everywhere it's set.
//
// 2. What happens if the socket connection happens to complete just as cancellation is
//    requested? I had to look at the source code of ConnectionTask because the docs don't
//    specify all cancellation semantics (sound familiar?). It turns out that, if it has
//    already completed, then cancel() is silently discarded. We actually depend on this
//    behaviour, because we call cancel() unconditionally in onCancel, even if we've got to
//    the reading part. But, if readBytes() has been scheduled but not run with its result,
//    subscription will still be null (hasn't been created yet) so won't be cancelled. So the
//    whole function will carry on, silently ignoring the cancellation!
//    CancelableOperation.valueOrCancellation will still return straight away so you won't be
//    able tell. At best, you leak a network resource; in a more complex situation, a function
//    with side effects might continue even though it's been cancelled.
//
// Here is the fixed code. But is it really fixed? Would you feel comforable putting this in
// production?
CancelableOperation<Uint8List> readBytesPossiblyFixed(String host, int port, int byteCount) {
  ConnectionTask<Socket>? connectTask;
  StreamSubscription<Uint8List>? subscription;
  Completer<void>? readDone;

  final completer = CancelableCompleter<Uint8List>(
    onCancel: () {
      connectTask?.cancel();
      subscription?.cancel();
      if (readDone != null && !readDone!.isCompleted) {  // fix for bug 1
        readDone!.completeError(
          SocketException('Operation cancelled'),
        );
      }
    },
  );

  () async {
    Socket? socket;
    try {
      connectTask = await Socket.startConnect(host, port);
      socket = await connectTask!.socket;

      if (completer.isCanceled) {
        throw SocketException('Operation cancelled');  // fix for bug 2
      }

      final buffer = BytesBuilder(copy: false);
      readDone = Completer<void>();

      subscription = socket.listen(
        (chunk) {
          buffer.add(chunk);
          if (buffer.length >= byteCount && !readDone!.isCompleted) {  // fix for bug 1
            readDone!.complete();
          }
        },
        onError: (e, st) {
          if (!readDone!.isCompleted) readDone!.completeError(e, st);  // fix for bug 1
        },
        onDone: () {
          if (!readDone!.isCompleted) {  // fix for bug 1
            readDone!.completeError(
              SocketException('Connection closed before $byteCount bytes received'),
            );
          }
        },
        cancelOnError: true,
      );

      await readDone!.future;

      completer.complete(buffer.takeBytes());
    } catch (e, st) {
      completer.completeError(e, st);
    } finally {
      socket?.destroy();
    }
  }();

  return completer.operation;
}

// --- Basic runner ---
//
// Spins up a local server that sends some bytes, then exercises both versions
// of readBytes(). This just calls the functions; it does not try to provoke the
// bugs in the original version.
void main() async {
  final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  server.listen((socket) {
    socket.add(Uint8List(100));
    socket.flush().then((_) => socket.destroy());
  });

  final host = server.address.address;
  final port = server.port;

  final original = await readBytes(host, port, 100).value;
  print('readBytes got ${original.length} bytes');

  final fixed = await readBytesPossiblyFixed(host, port, 100).value;
  print('readBytesPossiblyFixed got ${fixed.length} bytes');

  await server.close();
}
