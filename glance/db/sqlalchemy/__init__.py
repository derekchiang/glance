import logging
import sqlalchemy
import sqlalchemy.orm as sa_orm

from glance.common import exception
from glance.db.sqlalchemy import migration
from glance.db.sqlalchemy import models
import glance.openstack.common.log as os_logging


_ENGINE = None
_MAKER = None
_MAX_RETRIES = None
_RETRY_INTERVAL = None
BASE = models.BASE
sa_logger = None
LOG = os_logging.getLogger(__name__)

def get_engine():
    """Return a SQLAlchemy engine."""
    """May assign _ENGINE if not already assigned"""
    global _ENGINE, sa_logger, _CONNECTION, _IDLE_TIMEOUT, _MAX_RETRIES,\
        _RETRY_INTERVAL

    if not _ENGINE:
        tries = _MAX_RETRIES
        retry_interval = _RETRY_INTERVAL

        connection_dict = sqlalchemy.engine.url.make_url(_CONNECTION)

        engine_args = {
            'pool_recycle': _IDLE_TIMEOUT,
            'echo': False,
            'convert_unicode': True}

        try:
            _ENGINE = sqlalchemy.create_engine(_CONNECTION, **engine_args)

            if 'mysql' in connection_dict.drivername:
                sqlalchemy.event.listen(_ENGINE, 'checkout', _ping_listener)

            _ENGINE.connect = _wrap_db_error(_ENGINE.connect)
            _ENGINE.connect()
        except Exception as err:
            msg = _("Error configuring registry database with supplied "
                    "sql_connection. Got error: %s") % err
            LOG.error(msg)
            raise

        sa_logger = logging.getLogger('sqlalchemy.engine')
        if CONF.debug:
            sa_logger.setLevel(logging.DEBUG)

        if CONF.db_auto_create:
            LOG.info(_('auto-creating glance registry DB'))
            models.register_models(_ENGINE)
            try:
                migration.version_control()
            except exception.DatabaseMigrationError:
                # only arises when the DB exists and is under version control
                pass
        else:
            LOG.info(_('not auto-creating glance registry DB'))

    return _ENGINE


def _get_maker(autocommit=True, expire_on_commit=False):
    """Return a SQLAlchemy sessionmaker."""
    """May assign __MAKER if not already assigned"""
    global _MAKER, _ENGINE
    assert _ENGINE
    if not _MAKER:
        _MAKER = sa_orm.sessionmaker(bind=_ENGINE,
                                     autocommit=autocommit,
                                     expire_on_commit=expire_on_commit)
    return _MAKER

def get_session(autocommit=True, expire_on_commit=False):
    """Helper method to grab session"""
    global _MAKER
    if not _MAKER:
        get_engine()
        _get_maker(autocommit, expire_on_commit)
        assert(_MAKER)
    session = _MAKER()
    return session