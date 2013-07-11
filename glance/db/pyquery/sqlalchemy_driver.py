from query import QueryImplementation
from relations import OneToOne, OneToMany, ManyToOne, ManyToMany
from spec import *
from model import Model

from sqlalchemy.orm import joinedload
from sqlalchemy import asc, desc

from glance.db.sqlalchemy.api import _get_session

# engine = None  # Need to be set up
# Session = sessionmaker()
# Session.configure(bind=engine)
session = _get_session()

class InvalidOrderException(Exception):
    pass

class SQLAlchemyQueryImpl(QueryImplementation):

    @staticmethod
    def first(**kwargs):
        return SQLAlchemyQueryImpl.fetch(number=1, **kwargs)[0]

    @staticmethod
    def fetch(**kwargs):
        return SQLAlchemyQueryImpl.all(**kwargs)[:kwargs['number']]

    @staticmethod
    def all(**kwargs):
        model = kwargs['model']
        specs = kwargs['specs']
        joins = kwargs['joins']
        orders = kwargs['orders']
        query = session.query(model)

        join_filters = kwargs.get('join_filters')
        if join_filters:
            for join_name, spec in join_filters:
                query.join(join_name).filter(translate(spec))

        for spec in specs:
            if isinstance(spec.value_spec, EQ):
                query = query.filter(getattr(model, spec.attr) == spec.value_spec.value)
            elif isinstance(spec.value_spec, GT):
                query = query.filter(getattr(model, spec.attr) > spec.value_spec.value)
            elif isinstance(spec.value_spec, LT):
                query = query.filter(getattr(model, spec.attr) < spec.value_spec.value)
            elif isinstance(spec.value_spec, GTE):
                query = query.filter(getattr(model, spec.attr) >= spec.value_spec.value)
            elif isinstance(spec.value_spec, LTE):
                query = query.filter(getattr(model, spec.attr) <= spec.value_spec.value)

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
    def delete(model, specs, session):
        instances = SQLAlchemyQueryImpl.all(model, specs)
        for i in instances:
            session.delete(i)

        session.commit()

    @staticmethod
    def insert(model, specs, values, session):
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
