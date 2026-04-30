import 'dart:async';
import 'dart:collection';

/// Thrown when an operation is aborted.
class AbortException implements Exception {
  const AbortException();

  @override
  String toString() => 'AbortException';
}

// Node in AbortSignal's linked list of callbacks.
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

/// Represents a callback registered with AbortSignal; allows unregistering.
class AbortSignalRegistration {
  final _RegistrationEntry _entry;

  AbortSignalRegistration._(_RegistrationEntry entry) : _entry = entry;

  void unregister() {
    if (_entry.list == null) {
      throw StateError(
        'unregister() called on an already-unregistered AbortSignalRegistration',
      );
    }
    _entry.unlink();
  }
}

/// An abort signal, also known as a cancel token.
///
/// Allows checking whether already aborted and registering to be notified
/// when it is aborted. Created through the [AbortController] class or its
/// static methods [AbortSignal.timeout] and [AbortSignal.any].
class AbortSignal {
  bool _aborted = false;
  final LinkedList<_RegistrationEntry> _registrations = LinkedList();

  AbortSignal._();

  void _abort() {
    if (_aborted) {
      return;
    }
    _aborted = true;
    for (final entry in _registrations) {
      entry.schedule();
    }
  }

  bool get aborted => _aborted;

  void throwIfAborted() {
    if (_aborted) {
      throw const AbortException();
    }
  }

  /// Registers [callback] to be called as a microtask when this signal is
  /// aborted (or immediately scheduled, if already aborted).
  AbortSignalRegistration register(void Function() callback) {
    final entry = _RegistrationEntry(callback);
    _registrations.add(entry);
    if (_aborted) {
      entry.schedule();
    }
    return AbortSignalRegistration._(entry);
  }

  /// Returns a signal that aborts automatically after [duration].
  static AbortSignal timeout(Duration duration) {
    final signal = AbortSignal._();
    Timer(duration, signal._abort);
    return signal;
  }

  /// Returns a signal that aborts when any signal in [signals] aborts.
  static AbortSignal any(Iterable<AbortSignal> signals) {
    final combined = AbortSignal._();
    final registrations = <AbortSignalRegistration>[];

    void onAbort() {
      for (final reg in registrations) {
        reg.unregister();
      }
      combined._abort();
    }

    for (final signal in signals) {
      if (signal._aborted) {
        // _abort() is called directly rather than relying on register() to
        // schedule it, so that combined.aborted is set synchronously here
        // rather than in a later microtask.
        onAbort();
        return combined;
      }
      registrations.add(signal.register(onAbort));
    }

    return combined;
  }
}

/// Controller that allows aborting requests through its [AbortSignal].
class AbortController {
  final AbortSignal _signal = AbortSignal._();

  AbortSignal get signal => _signal;

  void abort() => _signal._abort();
}
