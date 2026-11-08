"""Per-thread instance attributes.

TableBuilder can run several documents at once on one PaperProcessor and one
LLM backend. Attributes that describe "the document or call in progress" must
then be kept per thread, or one document reads another's state.
"""

import threading
from typing import Any, Callable


class ThreadLocalAttribute:
    """Instance attribute whose value is separate in every thread.

    Each thread starts from ``default_factory()``. Works on objects created
    with ``object.__new__`` too, since the per-thread storage is created on
    first access.
    """

    _STORAGE = "_thread_local_state"

    def __init__(self, default_factory: Callable[[], Any]):
        self._default_factory = default_factory
        self._name = ""

    def __set_name__(self, owner, name):
        self._name = name

    def _storage(self, obj) -> threading.local:
        storage = obj.__dict__.get(self._STORAGE)
        if storage is None:
            # dict.setdefault is atomic, so two threads cannot create two
            # different storages for the same object.
            storage = obj.__dict__.setdefault(self._STORAGE, threading.local())
        return storage

    def __get__(self, obj, owner=None):
        if obj is None:
            return self
        storage = self._storage(obj)
        try:
            return getattr(storage, self._name)
        except AttributeError:
            value = self._default_factory()
            setattr(storage, self._name, value)
            return value

    def __set__(self, obj, value):
        setattr(self._storage(obj), self._name, value)
