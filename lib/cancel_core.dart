import 'dart:async';
import 'dart:collection';

/// Thrown when an operation is cancelled.
class CancelException implements Exception {
  const CancelException();

  @override
  String toString() => 'CancelException';
}

/// Signals a programming error: an operation threw [CancelException] when its
/// [CancelToken] was never cancelled. A [CancelException] must only be raised
/// in response to the token actually being cancelled.
class StrayCancelError extends Error {
  final String message;
  StrayCancelError(this.message);

  @override
  String toString() => 'StrayCancelError: $message';
}

// Node in CancelToken's linked list of callbacks.
base class _RegistrationEntry extends LinkedListEntry<_RegistrationEntry> {
  final void Function() callback;
  _RegistrationEntry(this.callback);

  void schedule() {
    scheduleMicrotask(() {
      // This last-moment check (list != null) is to handle the case where
      // unregister() was called between this entry being scheduled and running.
      if (list != null) {
        callback();
      }
    });
  }
}

/// Represents a callback registered with CancelToken; allows unregistering.
class CancelTokenRegistration {
  final _RegistrationEntry _entry;

  CancelTokenRegistration._(_RegistrationEntry entry) : _entry = entry;

  void unregister() {
    if (_entry.list == null) {
      throw StateError(
        'unregister() called on an already-unregistered CancelTokenRegistration',
      );
    }
    _entry.unlink();
  }
}

/// A cancellation token.
///
/// Allows checking whether already cancelled and registering to be notified
/// when it is cancelled. Obtained from a [CancelController], or via the body
/// callback of [CancelScope.using].
class CancelToken {
  bool _cancelled = false;
  final LinkedList<_RegistrationEntry> _registrations = LinkedList();

  CancelToken._();

  void _cancel() {
    if (_cancelled) {
      return;
    }
    _cancelled = true;
    for (final entry in _registrations) {
      entry.schedule();
    }
  }

  bool get cancelled => _cancelled;

  void throwIfCancelled() {
    if (_cancelled) {
      throw const CancelException();
    }
  }

  /// Registers [callback] to be called as a microtask when this token is
  /// cancelled (or immediately scheduled, if already cancelled).
  CancelTokenRegistration register(void Function() callback) {
    final entry = _RegistrationEntry(callback);
    _registrations.add(entry);
    if (_cancelled) {
      entry.schedule();
    }
    return CancelTokenRegistration._(entry);
  }
}

/// Controller that allows cancelling requests through its [CancelToken].
///
/// If [timeout] is supplied, [cancel] is called automatically after that
/// duration. If [linkedCancelTokens] is supplied, [cancel] is called when any of
/// them cancels. In both cases, calling [cancel] cancels the timer and removes
/// registrations on linked tokens, avoiding leaks.
class CancelController {
  final CancelToken _cancelToken = CancelToken._();
  Timer? _timer;
  List<CancelTokenRegistration>? _linkedRegistrations;

  CancelController({Duration? timeout, Iterable<CancelToken>? linkedCancelTokens}) {
    if (timeout != null) {
      _timer = Timer(timeout, cancel);
    }
    if (linkedCancelTokens != null) {
      final regs = _linkedRegistrations = [];
      for (final cancelToken in linkedCancelTokens) {
        if (cancelToken.cancelled) {
          cancel();
          return;
        }
        regs.add(cancelToken.register(cancel));
      }
    }
  }

  CancelToken get cancelToken => _cancelToken;

  void cancel() {
    _timer?.cancel();
    _timer = null;
    _linkedRegistrations?.forEach((reg) => reg.unregister());
    _linkedRegistrations = null;
    _cancelToken._cancel();
  }
}

/// A cancel scope: owns a [CancelToken], optionally listens to a parent
/// [CancelToken], and optionally fires after a timeout.
///
/// The token is only accessible from inside the [using] callback. The scope
/// is live from construction — the timeout (if any) is already running, and
/// the parent registration is already attached. When the timer expires or the
/// parent cancels, the scope's token is cancelled; the scope itself absorbs that
/// cancellation silently (use [cancelCaught] to detect it after [using] returns).
/// [withTimeout] wraps this with an explicit [TimeoutException] for the
/// deadline-bounded case.
class CancelScope {
  final CancelToken _cancelToken = CancelToken._();
  final CancelToken? _parentCancelToken;
  Timer? _timer;
  CancelTokenRegistration? _parentRegistration;
  bool _cancelCaught = false;
  bool _bodyStarted = false;
  bool _exited = false;

  /// Creates a scope. If [parentCancelToken] is supplied, cancelling it also
  /// cancels this scope's token. If [timeout] is supplied, the scope is
  /// cancelled silently after that duration — the body sees its token become
  /// cancelled and the exit logic treats the resulting [CancelException] the
  /// same as a manual [cancel]. Use [withTimeout] when you want a
  /// [TimeoutException] raised automatically on timeout.
  CancelScope({
    CancelToken? parentCancelToken,
    Duration? timeout,
  }) : _parentCancelToken = parentCancelToken {
    if (parentCancelToken != null) {
      if (parentCancelToken.cancelled) {
        _cancelToken._cancel();
      } else {
        _parentRegistration = parentCancelToken.register(_cancelToken._cancel);
      }
    }
    if (timeout != null) setTimeout(timeout);
  }

  /// Runs [body] inside this scope and applies the scope's exit rules to
  /// whatever it raises.
  ///
  /// May be called only once per scope; subsequent calls throw [StateError].
  /// The body receives this scope's [CancelToken] as a parameter — that
  /// token is not otherwise accessible from outside the body.
  Future<void> using(Future<void> Function(CancelToken) body) async {
    if (_bodyStarted) {
      throw StateError(
        'CancelScope.using() has already been called on this scope',
      );
    }
    _bodyStarted = true;
    try {
      await body(_cancelToken);
    } on CancelException {
      // Body raised a CancelException. Either rethrow as the parent's cancel
      // or absorb it as this scope's own.
      if (!_cancelToken.cancelled) {
        throw StrayCancelError(
          'Body of CancelScope threw CancelException without its '
          'CancelToken being cancelled',
        );
      }
      if (_parentCancelToken?.cancelled ?? false) rethrow;
      _cancelCaught = true;
      // Absorb the cancel: fall through to finally and return without
      // rethrowing.
      return;
    } finally {
      _exited = true;
      _timer?.cancel();
      _timer = null;
      _parentRegistration?.unregister();
      _parentRegistration = null;
    }
  }

  /// Whether this scope absorbed its own cancellation, analogous to Trio's
  /// `CancelScope.cancelled_caught`. True if the body raised a
  /// [CancelException] while the parent token was not cancelled.
  bool get cancelCaught => _cancelCaught;

  /// Cancels this scope's token.
  void cancel() {
    _cancelToken._cancel();
  }

  /// Replaces any existing timeout with a new one.
  ///
  /// Does nothing once the body has finished and the scope has exited, or
  /// if the token has already been cancelled (by the parent, a prior
  /// timeout, or [cancel]).
  void setTimeout(Duration timeout) {
    if (_exited || _cancelToken.cancelled) return;
    _timer?.cancel();
    _timer = Timer(timeout, _cancelToken._cancel);
  }

  /// Runs [body] inside a fresh [CancelScope] with the given [timeout] and
  /// [parentCancelToken] and returns the body's result.
  ///
  /// Unlike the instance [using] method, the scope is never handed back to
  /// the caller — nothing outside can call `cancel()` on it. The body's token
  /// therefore only cancels via [timeout] expiring (which surfaces as
  /// [TimeoutException] if the body observes it) or [parentCancelToken]
  /// cancelling (which surfaces as [CancelException]). Both cancel paths throw
  /// rather than being absorbed, so the return type is unambiguously the
  /// body's [T].
  static Future<T> withTimeout<T>({
    required Future<T> Function(CancelToken) body,
    required Duration timeout,
    CancelToken? parentCancelToken,
  }) async {
    T? result;
    final scope = CancelScope(parentCancelToken: parentCancelToken, timeout: timeout);
    await scope.using((cancelToken) async {
      result = await body(cancelToken);
    });
    if (scope.cancelCaught) {
      throw TimeoutException('CancelScope.withTimeout timed out', timeout);
    }
    return result as T;
  }
}
