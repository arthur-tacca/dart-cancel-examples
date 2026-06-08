import 'dart:async';
import 'dart:collection';

/// Marker type whose only purpose is to be the runtime-type of the zone key
/// under which the ambient [CancelToken] is stored. The class is exposed so it
/// shows up by name in tooling/debugger output (e.g.
/// `Zone.current[CancelTokenZoneKey]` makes it obvious which library owns the
/// entry); the constructor is private, so the only instance that exists is
/// [_cancelTokenZoneKey] inside this library, and no other code can forge one
/// or shadow our entry by accident.
class CancelTokenZoneKey {
  const CancelTokenZoneKey._();
}

const Object _cancelTokenZoneKey = CancelTokenZoneKey._();

/// The ambient [CancelToken] for the current zone, or null if there isn't one.
///
/// Set by [CancelScope] when [CancelScope.using] forks a zone around its body.
/// [TaskGroup] inherits this through the [CancelScope] it composes, so all
/// tasks spawned in the group see the group's token as ambient. Cancellable
/// helpers like `sleep`, `waitCancellable`, `streamCancellable`,
/// `connectSocket` and `connectSecureSocket` read this directly.
CancelToken? get currentCancelToken =>
    Zone.current[_cancelTokenZoneKey] as CancelToken?;

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

/// A cancel token.
///
/// Allows checking whether already cancelled and registering to be notified
/// when it is cancelled. Inside a [CancelScope.using] body it is the ambient
/// [currentCancelToken].
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

/// A cancel scope: owns a [CancelToken], inherits cancellation from the
/// enclosing scope (via the ambient [currentCancelToken]), and optionally fires
/// after a timeout. The scope's exit logic inspects whatever the body raised
/// and decides whether to absorb its own cancellation, re-raise it, or replace
/// it with a [TimeoutException].
///
/// When [using] runs the body, it forks a zone in which [currentCancelToken] is
/// this scope's token — cancellable helpers like `sleep` and `connectSocket`
/// pick it up without needing any parameter threading. [cancelCaught] reflects
/// the final state once [using] returns.
class CancelScope {
  final CancelToken _cancelToken = CancelToken._();
  final CancelToken? _parentCancelToken;
  final bool _shield;
  Timer? _timer;
  CancelTokenRegistration? _parentRegistration;
  bool _cancelCaught = false;
  bool _bodyStarted = false;
  bool _exited = false;

  /// Creates a scope. The parent token is the ambient [currentCancelToken] at
  /// construction time, so a scope created inside another scope's body
  /// inherits cancellation automatically. If [shield] is true, the scope does
  /// **not** link to the ambient token — its body runs uncancelled by the
  /// parent. This is analogous to Trio's `CancelScope.shield` and lets you run
  /// cleanup or otherwise-required work from inside an already-cancelled scope.
  ///
  /// If [timeout] is supplied, the scope is cancelled after that duration. The
  /// scope absorbs its own timeout — [using] returns normally with
  /// [cancelCaught] set to true. Callers that want a thrown [TimeoutException]
  /// on timeout should use [CancelScope.withTimeout] or [TaskGroup.waitAll] /
  /// [TaskGroup.waitAny], which detect the timeout themselves.
  CancelScope({
    bool shield = false,
    Duration? timeout,
  })  : _parentCancelToken = shield ? null : currentCancelToken,
        _shield = shield {
    final pt = _parentCancelToken;
    if (pt != null) {
      if (pt.cancelled) {
        _cancelToken._cancel();
      } else {
        _parentRegistration = pt.register(_cancelToken._cancel);
      }
    }
    if (timeout != null) {
      setTimeout(timeout);
    }
  }

  /// Runs [body] inside this scope and applies the scope's exit rules to
  /// whatever it raises.
  ///
  /// The body runs in a forked zone in which [currentCancelToken] resolves to
  /// this scope's token. May be called only once per scope; subsequent calls
  /// throw [StateError].
  Future<void> using(Future<void> Function() body) async {
    if (_bodyStarted) {
      throw StateError(
        'CancelScope.using() has already been called on this scope',
      );
    }
    _bodyStarted = true;
    try {
      await Zone.current
          .fork(zoneValues: {_cancelTokenZoneKey: _cancelToken})
          .run(body);
    } on CancelException {
      // Body raised a CancelException. Either propagate it as the parent's
      // cancellation or absorb it as our own.
      if (!_cancelToken.cancelled) {
        throw StrayCancelError(
          'Body of CancelScope threw CancelException without its '
          'CancelToken being cancelled',
        );
      }
      if (_parentCancelToken?.cancelled ?? false) rethrow;
      _cancelCaught = true;
      // Absorb our own cancellation: fall through to finally and return without
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

  /// Whether this scope was created with `shield: true`, in which case it did
  /// not link to the ambient parent token.
  bool get shield => _shield;

  /// Cancels this scope's token.
  void cancel() {
    _cancelToken._cancel();
  }

  /// Replaces any existing timeout with a new one.
  ///
  /// Does nothing once the body has finished and the scope has exited, or once
  /// the scope's token has already been cancelled (the timer would only call
  /// the now-no-op `_cancelToken._cancel()`).
  void setTimeout(Duration timeout) {
    if (_exited || _cancelToken.cancelled) return;
    _timer?.cancel();
    _timer = Timer(timeout, _cancelToken._cancel);
  }

  /// Runs [body] inside a fresh [CancelScope] with the given [timeout] and
  /// returns the body's result.
  ///
  /// Unlike the instance [using] method, the scope is never handed back to the
  /// caller — nothing outside can call `cancel()` on it. The body's token
  /// therefore only cancels via [timeout] expiring (which surfaces as
  /// [TimeoutException] if the body observes it) or the ambient parent token
  /// cancelling (which surfaces as [CancelException]). If [shield] is true, the
  /// body does not inherit the ambient parent token.
  static Future<T> withTimeout<T>({
    required Future<T> Function() body,
    required Duration timeout,
    bool shield = false,
  }) async {
    T? result;
    final scope = CancelScope(shield: shield, timeout: timeout);
    await scope.using(() async {
      result = await body();
    });
    if (scope.cancelCaught) {
      // No external canceller can reach this scope, so the only thing that
      // could have absorbed a cancellation is the timeout firing.
      throw TimeoutException('CancelScope.withTimeout timed out', timeout);
    }
    return result as T;
  }
}
