import 'dart:async';
import 'dart:collection';

/// Marker type whose only purpose is to be the runtime-type of the zone key
/// under which the ambient [AbortSignal] is stored. The class is exposed so
/// it shows up by name in tooling/debugger output (e.g.
/// `Zone.current[AbortSignalZoneKey]` makes it obvious which library owns the
/// entry); the constructor is private, so the only instance that exists is
/// [_abortSignalZoneKey] inside this library, and no other code can forge
/// one or shadow our entry by accident.
class AbortSignalZoneKey {
  const AbortSignalZoneKey._();
}

const Object _abortSignalZoneKey = AbortSignalZoneKey._();

/// The ambient [AbortSignal] for the current zone, or null if there isn't one.
///
/// Set by [TaskGroup] when a body or task runs inside one of its forked
/// zones. Cancellable helpers like `sleep`, `waitCancellable`,
/// `streamCancellable` and `connectSocket` read this directly.
AbortSignal? get currentSignal =>
    Zone.current[_abortSignalZoneKey] as AbortSignal?;

/// Returns a child of [Zone.current] in which [signal] is the ambient
/// [currentSignal]. The only intended caller is [TaskGroup]; it's exposed so
/// the implementation can keep the zone key itself library-private.
Zone forkZoneWithSignal(AbortSignal signal) =>
    Zone.current.fork(zoneValues: {_abortSignalZoneKey: signal});

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
/// In this design timeout-based and combined-signal cancellation are
/// expressed at the [TaskGroup] layer rather than on individual controllers
/// (a `TaskGroup`'s `timeout` parameter, plus nesting and explicit
/// `register(tg.abort)` calls inside a body), so [AbortController] itself is
/// just a thin owner of an [AbortSignal].
class AbortController {
  final AbortSignal _signal = AbortSignal._();

  AbortController();

  AbortSignal get signal => _signal;

  void abort() => _signal._abort();
}
