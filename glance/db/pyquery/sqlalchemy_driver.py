from query import QueryImplementation
from relations import OneToOne, OneToMany, ManyToOne, ManyToMany
from spec import *
from model import Model

from sqlalchemy.orm import joinedload
from sqlalchemy import asc, desc
from sqlalchemy import not_, or_, and_

from glance.db.pyquery.exceptions import UnsupportedSpecError, InvalidOrderException

from glance.db.sqlalchemy.exceptions import InvalidSpecError
from glance.db.sqlalchemy.api import _get_session
from glance.db.models import get_related_model

# engine = None  # Need to be set up
# Session = sessionmaker()
# Session.configure(bind=engine)
# session = _get_session()

def printquery(statement, bind=None):
    """
    print a query, with values filled in
    for debugging purposes *only*
    for security, you should always separate queries from their values
    please also note that this function is quite slow
    """
    import sqlalchemy.orm
    if isinstance(statement, sqlalchemy.orm.Query):
        if bind is None:
            bind = statement.session.get_bind(
                    statement._mapper_zero_or_none()
            )
        statement = statement.statement
    elif bind is None:
        bind = statement.bind 

    dialect = bind.dialect
    compiler = statement._compiler(dialect)
    class LiteralCompiler(compiler.__class__):
        def visit_bindparam(
                self, bindparam, within_columns_clause=False, 
                literal_binds=False, **kwargs
        ):
            return super(LiteralCompiler, self).render_literal_bindparam(
                    bindparam, within_columns_clause=within_columns_clause,
                    literal_binds=literal_binds, **kwargs
            )

    compiler = LiteralCompiler(dialect, statement)
    print compiler.process(statement)

class SQLAlchemyQueryImpl(QueryImplementation):

    query = None

    @classmethod
    def translate_spec(cls, model, spec):
        if isinstance(spec, Attr):
            attr = spec.attr

            # To deal with join queries like `members.name == "Derek"`
            if '.' in spec.attr:
                parts = spec.attr.split('.')
                ref_name = parts[0]
                attr = parts[1]
                model = get_related_model(model, ref_name)
                assert model is not None
                cls.query = cls.query.outerjoin(ref_name)

            if isinstance(spec.value_spec, EQ):
                return (getattr(model, attr) == spec.value_spec.value)
            elif isinstance(spec.value_spec, NEQ):
                return (getattr(model, attr) != spec.value_spec.value)  
            elif isinstance(spec.value_spec, GT):
                return (getattr(model, attr) > spec.value_spec.value)
            elif isinstance(spec.value_spec, LT):
                return (getattr(model, attr) < spec.value_spec.value)
            elif isinstance(spec.value_spec, GTE):
                return (getattr(model, attr) >= spec.value_spec.value)
            elif isinstance(spec.value_spec, LTE):
                return (getattr(model, attr) <= spec.value_spec.value)
            else:
                raise UnsupportedSpecError('WTF did you give me?')
        elif isinstance(spec, Not):
            return not_(cls.translate_spec(model, spec.spec))
        else:
            lst = []
            for sub_spec in spec.specs:
                lst.append(cls.translate_spec(model, sub_spec))
                # print "sub_spec"
                # print sub_spec
                # print type(cls.translate_spec(model, sub_spec))
            if isinstance(spec, And):
                # print "And: "
                # print lst
                return and_(*lst)
            elif isinstance(spec, Or):
                # print "Or: "
                # print lst
                return or_(*lst)
            else:
                raise InvalidSpecError()


    @classmethod
    def first(cls, *args, **kwargs):
        try:
            return cls.fetch(number=1, **kwargs)[0]
        except IndexError:
            return None

    @classmethod
    def fetch(cls, *args, **kwargs):
        number = None
        if len(args) > 0:
            number = args[0]
        else:
            number = kwargs['number']
        return cls.all(**kwargs)[:number]

    @classmethod
    def all(cls, *args, **kwargs):
        model = kwargs['model']
        specs = kwargs['specs']
        joins = kwargs['joins']
        orders = kwargs['orders']
        join_filters = kwargs.get('join_filters')

        cls.query = _get_session().query(model)

        for m, key in joins:
            cls.query = cls.query.options(joinedload(getattr(m, key)))

        if join_filters:
            for join_name, spec in join_filters:
                translated = cls.translate_spec(model, spec, query)
                cls.query = cls.query.join(join_name).filter(translated)

        print "Length of specs: "
        print len(specs)
        for spec in specs:
            # final_spec = cls.translate_spec(model, spec)
            # print final_spec
            # print type(final_spec)
            # print dir(final_spec)
            # printquery(query)
            translated = cls.translate_spec(model, spec)
            cls.query = cls.query.filter(translated)
            # printquery(query)

        for attr, order in orders:
            if order == 'asc':
                cls.query = cls.query.order_by(asc(getattr(model, attr)))
            elif order == 'desc':
                cls.query = cls.query.order_by(desc(getattr(model, attr)))
            else:
                raise InvalidOrderException('Order needs to be either "asc" or "desc"')


        return cls.query.all()

    @classmethod
    def delete(cls, *args, **kwargs):
        session = _get_session()

        model = kwargs['model']
        specs = kwargs['specs']
        instances = cls.all(model, specs)
        for i in instances:
            session.delete(i)

        session.commit()

    @classmethod
    def insert(cls, *args, **kwargs):
        session = _get_session()

        specs = kwargs['specs']
        model = kwargs['model']
        values = kwargs['values']

        if specs:
            instances = cls.all(model, specs)
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
