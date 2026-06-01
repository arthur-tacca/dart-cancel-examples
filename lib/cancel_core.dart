import 'dart:async';
import 'dart:collection';

/// Thrown when an operation is cancelled.
class CancelException implements Exception {
  const CancelException();

  @override
  String toString() => 'CancelException';
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
/// when it is cancelled. Obtained from a [CancelController].
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
