from query import QueryImplementation

from pycassa.columnfamilymap import ColumnFamilyMap
# from pycassa.index import create_index_expression, create_index_clause
import pycassa.index as index
from pycassa.index import create_index_clause, create_index_expression

from spec import Attr, EQ, GT, LT, GTE, LTE, NEQ, And, Or

# TODO: set it up
pool = None

class CassandraQueryImpl(QueryImplementation):
    @classmethod
    def translate_spec(cls, model, spec):
        expressions = []

        for spec in specs:
            if isinstance(spec.value_spec, EQ):
                expressions.push(
                    create_index_expression(spec.attr, spec.value_spec.value))
            elif isinstance(spec.value_spec, GT):
                expressions.push(create_index_expression(
                    spec.attr, spec.value_spec.value, index.GT))
            elif isinstance(spec.value_spec, LT):
                expressions.push(create_index_expression(
                    spec.attr, spec.value_spec.value, index.LT))
            elif isinstance(spec.value_spec, GTE):
                expressions.push(create_index_expression(
                    spec.attr, spec.value_spec.value, index.GTE))
            elif isinstance(spec.value_spec, LTE):
                expressions.push(create_index_expression(
                    spec.attr, spec.value_spec.value, index.LTE))

        cfm = ColumnFamilyMap(model, pool, model.cf_name)
        clause = create_index_clause([state_expr, bday_expr], count=number)

        def value_only(lst):
            values = []
            for key, value in lst:
                values.push(value)
            return values

        return value_only(cfm.get_indexed_slices(clause))

    @classmethod
    def first(cls, *args, **kwargs):
        try:
            return cls.fetch(number=1, *args, **kwargs)[0]
        except IndexError:
            return None

    @classmethod
    def fetch(cls, *args, **kwargs):
        model = kwargs['model']
        specs = kwargs['specs']
        joins = kwargs['joins']
        orders = kwargs['orders']
        join_filters = kwargs.get('join_filters')


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

        for spec in specs:
            translated = cls.translate_spec(model, spec)
            cls.query = cls.query.filter(translated)

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

class InvertedIndicesRepo(object):
    