import glance.openstack.common.log as os_logging
from glance.openstack.common import timeutils, uuidutils

from pycassa.columnfamily import ColumnFamily
from pycassa.index import EQ, GT, GTE, LT, LTE, \
                          create_index_expression, \
                          create_index_clause
from pycassa import index, NotFoundException

import pickle

LOG = os_logging.getLogger(__name__)

MARSHAL_PREFIX = '__'

# Custom exceptions
class ImageIdNotFoundException(Exception):
    pass

class ImageIdDuplicateException(Exception):
    pass

class ImageIdNotGivenException(Exception):
    pass

class UndefinedModelException(Exception):
    pass

class CassandraRepo(object):
    def __init__(self, pool):
        self.pool = pool

# Utility function for defining enums
# Source: http://stackoverflow.com/a/1695250/1563568
def enum(**enums):
    return type('Enum', (), enums)

Models = enum(Image=0, ImageMember=1, ImageLocation=2,\
              ImageProperty=3, ImageTag=4)

def merge_dict(dict1, dict2):
    return dict(dict1, **dict2)

def sort_dicts(dicts, sort_by):
    # sort_by is a list of tuples of the form:
    # [('name', 'asc'), ('age', 'des')]
    def insertion_sort(dicts, sort_by):
        pass

    def quick_sort(dicts, sort_by):
        pass

    if len(dicts) < 20:
        insertion_sort(dicts, sort_by)
    else:
        quick_sort(dicts, sort_by)

__protected_attributes__ = set([
        "created_at", "updated_at", "deleted_at", "deleted"])

__serialized_fields__ = set([
        'members', 'properties', 'tags', 'locations'])

def drop_protected_attrs(vals):
        for attr in __protected_attributes__:
            if attr in vals:
                del vals[attr]
                
# TODO: create a new repo every time instead of reset
class ImageRepo(object):
    def __init__(self, pool):
        self.pool = pool
        self.cf = ColumnFamily(pool, 'Images', dict_class=dict)
        self.inverted_cf = ColumnFamily(pool, 'Inverted_indices', dict_class=dict)

    def soft_delete(self, obj, model=Models.Image):
        obj.deleted = True
        obj.deleted_at = timeutils.utcnow()
        self.save(obj, model, override=True)

        # TODO

    @staticmethod
    def marshal(obj):
        for k, v in obj.iteritems():
            if k in __serialized_fields__:
                obj[k] = pickle.dumps(v)
            elif not isinstance(v, basestring):
                obj[k] = MARSHAL_PREFIX + pickle.dumps(v)

        return obj

    @staticmethod
    def unmarshal(obj, loads=None):
        for k, v in obj.iteritems():
            if isinstance(v, basestring):
                if v.startswith(MARSHAL_PREFIX):
                    obj[k] = pickle.loads(v[len(MARSHAL_PREFIX):])

        if loads:
            for load in loads:
                l = obj.get(load)
                if l:
                    obj[load] = pickle.loads(l)
                else:
                    obj[load] = []

        return obj

    def save(self, obj, model=Models.Image, override=False):
        # TODO: should we do copy() or deepcopy()?
        obj = obj.copy()

        if model == Models.Image:
            key = obj.get('key') or obj.get('id')
            
            if key is None:
                raise ImageIdNotGivenException()

            if override is False:
                try:
                    self.cf.get(key)
                    raise ImageIdDuplicateException()
                except NotFoundException:
                    pass

            # Cassandra can only save strings, so
            # we need to marshal None and boolean
            self.marshal(obj)

            LOG.info('obj: ')
            LOG.info(key)
            LOG.info(obj)
            self.cf.insert(key, obj)
        else:
            prefix = {
                Models.ImageMember: 'members',
                Models.ImageLocation: 'locations',
                Models.ImageProperty: 'properties',
                Models.ImageTag: 'tags'
            }.get(model)

            image_id = obj.pop('image_id')

            # Save all attributes as inverted indices
            for k, v in obj.iteritems():
                # row key would be something like:
                # members.status=pending
                row_key = prefix + '.' + str(k) + '=' + str(v)

                # TODO: just append a column
                try:
                    original = self.inverted_cf.get(row_key)
                    new_dict = merge_dict(original, {image_id: ''})
                except NotFoundException:
                    new_dict = {image_id: ''}
                
                self.inverted_cf.insert(row_key, new_dict)

            # Save obj as serialized data
            image = self.cf.get(image_id)
            arr = image.get(prefix)

            print "I'm getting the image!!"
            print image

            if arr:
                print 'prefix: '
                print prefix
                print arr
                arr = pickle.loads(arr)
                if override:
                    # look for an existing object with the same id
                    overriden = False
                    for i, elem in enumerate(arr):
                        if obj.get('id') and obj.get('id') == elem.get('id'):
                            arr[i] = obj
                            overriden = True
                            break
                    if not overriden:
                        arr.append(obj)
                else:
                    arr.append(obj)
            else:
                arr = [obj]

            strarr = pickle.dumps(arr)
            image[prefix] = strarr

            self.cf.insert(image_id, image)


    def reset(self):
        self.expressions = []
        self.loads = []

    def load(self, load):
        self.loads.append(load)
        return self


    # This method supports multiple usage pattern, including:
    # 1. filter(name='derek', age='20')
    # 2. filter(name='derek', (age, '>', 20))
         # filter the image(s) with the given attributes
    def filter(self, *args, **kwargs):
        for arg in args:
            if isinstance(arg, tuple):
                column_name, operator, value = arg

                op = {
                    '=': index.EQ,
                    '>': index.GT,
                    '>=': index.GTE,
                    '<': index.LT,
                    '<=': index.LTE
                }.get(operator)

                assert op is not None

                obj = {column_name: value}
                self.marshal(obj)

                self.expressions.append(create_index_expression(column_name,
                                                           obj[column_name], op))

        for k, v in kwargs.iteritems():
            obj = {k: v}
            self.marshal(obj)
            self.expressions.append(create_index_expression(k, obj[k], index.EQ))

        return self

    # TODO: This is a hack to get all records.  See if there is a
    # more elegant way.
    def get_all(self, *args, **kwargs):
        kwargs['number'] = 99999999
        return self.get(*args, **kwargs)

    def get(self, number=1, key=None):
        if key and isinstance(key, basestring):
            res = self.cf.get(key)

            self.unmarshal(res, self.loads)

            print res
            return res
        elif self.expressions != []:
            clause = create_index_clause(self.expressions, count=number)
            res = self.cf.get_indexed_slices(clause)

            results = []
            for item in res:
                key, columns = item
                results.append(self.unmarshal(columns, self.loads))

            return results
