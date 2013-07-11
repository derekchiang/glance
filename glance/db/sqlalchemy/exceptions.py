from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError

class NoResultFoundError(NoResultFound):
    pass

class DuplicateKeyError(IntegrityError):
    pass

class InvalidSpecError(Exception):
    pass