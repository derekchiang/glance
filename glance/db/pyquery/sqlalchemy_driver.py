from query import QueryImplementation
from relations import OneToOne, OneToMany, ManyToOne, ManyToMany
from spec import *
from model import Model

from sqlalchemy.orm import joinedload
from sqlalchemy import asc, desc
from sqlalchemy import not_, or_, and_

from glance.db.sqlalchemy.exceptions import NoResultFoundError, InvalidSpecError

from glance.db.sqlalchemy.api import _get_session

# engine = None  # Need to be set up
# Session = sessionmaker()
# Session.configure(bind=engine)
session = _get_session()

class InvalidOrderException(Exception):
    pass

class SQLAlchemyQueryImpl(QueryImplementation):

    @staticmethod
    def translate_spec(model, spec):
        if isinstance(spec, Attr):
            if isinstance(spec.value_spec, EQ):
                return (getattr(model, spec.attr) == spec.value_spec.value)
            elif isinstance(spec.value_spec, GT):
                return (getattr(model, spec.attr) > spec.value_spec.value)
            elif isinstance(spec.value_spec, LT):
                return (getattr(model, spec.attr) < spec.value_spec.value)
            elif isinstance(spec.value_spec, GTE):
                return (getattr(model, spec.attr) >= spec.value_spec.value)
            elif isinstance(spec.value_spec, LTE):
                return (getattr(model, spec.attr) <= spec.value_spec.value)
        elif isinstance(spec, Not):
            return not_(SQLAlchemyQueryImpl.translate_spec(model, spec.spec))
        else:
            lst = []
            for sub_spec in spec.specs:
                lst.append(SQLAlchemyQueryImpl.translate_spec(model, sub_spec))
            if isinstance(spec, And):
                return and_(*lst)
            elif isinstance(spec, Or):
                return or_(*lst)
            else:
                raise InvalidSpecError()


    @staticmethod
    def first(*args, **kwargs):
        try:
            return SQLAlchemyQueryImpl.fetch(number=1, **kwargs)[0]
        except IndexError:
            return None

    @staticmethod
    def fetch(*args, **kwargs):
        return SQLAlchemyQueryImpl.all(**kwargs)[:kwargs['number']]

    @staticmethod
    def all(*args, **kwargs):
        model = kwargs['model']
        specs = kwargs['specs']
        joins = kwargs['joins']
        orders = kwargs['orders']
        query = session.query(model)

        join_filters = kwargs.get('join_filters')
        if join_filters:
            for join_name, spec in join_filters:
                query.join(join_name).filter(SQLAlchemyQueryImpl.translate_spec(model, spec))

        for spec in specs:
            query = query.filter(SQLAlchemyQueryImpl.translate_spec(model, spec))

        for m, key in joins:
            query = query.options(joinedload(getattr(m, key)))

        for attr, order in orders:
            if order == 'asc':
                query = query.order_by(asc(model[attr]))
            elif order == 'desc':
                query = query.order_by(desc(model[attr]))
            else:
                raise InvalidOrderException('Order needs to be either "asc" or "desc"')

        return query.all()

    @staticmethod
    def delete(*args, **kwargs):
        model = kwargs['model']
        specs = kwargs['specs']
        instances = SQLAlchemyQueryImpl.all(model, specs)
        for i in instances:
            session.delete(i)

        session.commit()

    @staticmethod
    def insert(*args, **kwargs):
        specs = kwargs['specs']
        model = kwargs['model']
        values = kwargs['values']

        if specs:
            instances = SQLAlchemyQueryImpl.all(model, specs)
            for i in instances:
                for k, v in values:
                    i[k] = v
                session.add(i)
        else:
            # if specs is empty, then we insert a new instance
            # rather than replace old ones
            new_instance = model()
            for k, v in values:
                new_instance[k] = v
            session.add(i)

        session.commit()
