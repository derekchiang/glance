# TODO: move these functions to a more appropriate place

from glance.db.cassandra.models import register_models, unregister_models

def setup_db_env():
    register_models()

def clear_db_env():
    unregister_models()

def _get_session():
    class DummySession(object):
        def __enter__(self):
            return None

        def __exit__(self, type, value, traceback):
            pass

    class DummySessionManager(object):
        def begin(self):
            return DummySession()

    return DummySessionManager()