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
/// when it is aborted. Obtained from an [AbortController].
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
}

/// Controller that allows aborting requests through its [AbortSignal].
///
/// If [timeout] is supplied, [abort] is called automatically after that
/// duration. If [linkedSignals] is supplied, [abort] is called when any of
/// them aborts. In both cases, calling [abort] cancels the timer and removes
/// registrations on linked signals, avoiding leaks.
class AbortController {
  final AbortSignal _signal = AbortSignal._();
  Timer? _timer;
  List<AbortSignalRegistration>? _linkedRegistrations;

  AbortController({Duration? timeout, Iterable<AbortSignal>? linkedSignals}) {
    if (timeout != null) {
      _timer = Timer(timeout, abort);
    }
    if (linkedSignals != null) {
      final regs = _linkedRegistrations = [];
      for (final signal in linkedSignals) {
        if (signal.aborted) {
          abort();
          return;
        }
        regs.add(signal.register(abort));
      }
    }
  }

  AbortSignal get signal => _signal;

  void abort() {
    _timer?.cancel();
    _timer = null;
    _linkedRegistrations?.forEach((reg) => reg.unregister());
    _linkedRegistrations = null;
    _signal._abort();
  }
}
