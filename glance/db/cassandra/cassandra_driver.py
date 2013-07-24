from pycassa.columnfamilymap import ColumnFamilyMap
from pycassa import ConnectionPool
# from pycassa.index import create_index_expression, create_index_clause
import pycassa.index as index
from pycassa.index import create_index_clause, create_index_expression
from glance.db.pyquery.spec import Attr, EQ, GT, LT, GTE, LTE, NEQ, And, Or
from glance.db.pyquery.query import QueryImplementation
from glance.db.models import get_related_model
from glance.db.cassandra.models import secondary_index_repo

# TODO: set it up
class CassandraQueryImpl(QueryImplementation):

    @staticmethod
    def translate_spec(spec):
        if isinstance(spec.value_spec, EQ):
            return create_index_expression(spec.attr, spec.value_spec.value, index.EQ)
        elif isinstance(spec.value_spec, GT):
            return create_index_expression(spec.attr, spec.value_spec.value, index.GT)
        elif isinstance(spec.value_spec, LT):
            return create_index_expression(spec.attr, spec.value_spec.value, index.LT)
        elif isinstance(spec.value_spec, GTE):
            return create_index_expression(spec.attr, spec.value_spec.value, index.GTE)
        elif isinstance(spec.value_spec, LTE):
            return create_index_expression(spec.attr, spec.value_spec.value, index.LTE)

    @staticmethod
    def value_only(lst):
        values = []
        for key, value in lst:
            values.push(value)
        return values

    @classmethod
    def get_data(cls, model, spec, join_filter):
        # TODO: clean up this code

        # TODO: currently, we use ids as keys.  One potential optimization
        # would be to inspect if the client is filtering on ids, and if so,
        # simply get all rows associated with those keys, rather than using
        # column indices

        pool = ConnectionPool('GLANCE')

        if join_filter:
            ref_name, spec = join_filter
            related_model = get_related_model(model, ref_name)
            cfm, parents, _ = secondary_index_repo.get_data(related_model, spec)
            for p in parents:
                d = cfm.get(key=p)
                yield d

        elif spec:
            if model.is_standalone:
                cfm = ColumnFamilyMap(model, pool, model.cf_name)
                expressions = []

                if isinstance(spec, Or):
                    for sub_spec in spec.specs:
                        for i in cls.get_data(model, sub_spec, None):
                            yield i
                elif isinstance(spec, And):
                    for sub_spec in spec.specs:
                        expressions.append(cls.translate_spec(sub_spec))
                else:
                    expressions.append(cls.translate_spec(spec))
                
                clause = create_index_clause(expressions)
                for v in cls.value_only(cfm.get_indexed_slices(clause)):
                    yield v

            else:
                cfm, parents, related_name = secondary_index_repo.get_data(model, spec)
                for p in parents:
                    d = cfm.get(key=p, columns=[related_name])
                    for child in d[related_name]:
                        if spec.match(child):
                            yield child
        else:
            raise Exception('I need to have either a spec or a join filter')


    @classmethod
    def first(cls, *args, **kwargs):
        try:
            return cls.fetch(number=1, *args, **kwargs)[0]
        except IndexError:
            return None

    @classmethod
    def fetch(cls, *args, **kwargs):
        number = kwargs['number']

        # TODO: it doesn't really make sense to fetch all
        # matches and then only get part of it.
        # You should use `fetch` to implement `all`, not the
        # other way around
        return cls.all(*args, **kwargs)[:number]


    @classmethod
    def all(cls, *args, **kwargs):
        model = kwargs.get('model')
        specs = kwargs.get('specs')
        orders = kwargs.get('orders')
        join_filters = kwargs.get('join_filters')

        vals = []

        # Convert some specs to join filters
        # Specifically, specs of the form Attr('a.b', EQ(c))
        # are converted to ('a', Attr('b', EQ(c)))
        _specs = []
        for spec in specs:
            if isinstance(spec, Attr) and ('.' in spec.attr):
                parts = spec.attr.split('.')
                join_filters.append((parts[0], Attr(parts[1],\
                                     spec.value_spec)))
            else:
                _specs.append(spec)
        specs = _specs


        # Use the first spec to get a list of instances from
        # the database, and then use the rest of filters on
        # the client side
        if specs:
            vals.extend(cls.get_data(model, specs[0], None))
            specs = specs[1:]

        elif join_filters:
            join_name, spec = join_filters[0]
            vals.extend(cls.get_data(model, None, join_filter=join_filters[0]))
            join_filters = join_filters[1:]

        return cls.local_filter(vals, specs, join_filters, orders)

    @staticmethod
    def local_filter(vals, specs, join_filters, orders):
        spec = And(*specs)

        intermediate_results = []
        for val in vals:
            if spec.match(val):
                intermediate_results.append(val)

        results = []
        for val in intermediate_results:
            matched = True
            for ref_name, spec in join_filters:
                attr = getattr(val, ref_name)
                if not spec.match(attr):
                    matched = False

            if matched:
                results.append(val)

        return results


    @classmethod
    def insert(cls, *args, **kwargs):
        specs = kwargs['specs']
        model = kwargs['model']
        values = kwargs['values']

        if specs:
            instances = cls.all(model, specs)
            for instance in instances:
                instance = model(instance)
                for k, v in values:
                    instance[k] = v
                instance.save()
        else:
            # if specs is empty, then we insert a new instance
            # rather than replace old ones
            new_instance = model()
            for k, v in values:
                new_instance[k] = v
            new_instance.save()
