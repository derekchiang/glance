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

class ImageRepo(object):
    def __init__(self, pool):
        self.pool = pool
        self.cf = ColumnFamily(pool, 'Images', dict_class=dict)
        self.inverted_cf = ColumnFamily(pool, 'Inverted_indices', dict_class=dict)

    @staticmethod
    def create(model, **kwargs):
        # Create the given model
        base = merge_dict({
            'created_at': timeutils.utcnow(),
            'updated_at': timeutils.utcnow(),
            'deleted': False,
            'checksum': None,
            'disk_format': None,
            'container_format': None
        }, kwargs)

        if model == Models.Image:
            return merge_dict(base, {
                'id': uuidutils.generate_uuid(),
                'is_public': False,
                'min_disk': 0,
                'min_ram': 0,
                'protected': False
            })

        elif model == Models.ImageMember:
            return merge_dict(base, {
                'can_share': False,
                'status': 'pending'
            })

        elif model == Models.ImageLocation:
            return merge_dict(base, {
                'meta_data': {}
            })

        elif model == Models.ImageProperty:
            return merge_dict(base, {})

        elif model == Models.ImageTag:
            return merge_dict(base, {})

        else:
            raise UndefinedModelException()

    def soft_delete(self, obj, model=Models.Image):
        obj.deleted = True
        obj.deleted_at = timeutils.utcnow()
        self.save(obj, model, override=True)

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
            for k, v in obj.iteritems():
                if k in __serialized_fields__:
                    obj[k] = pickle.dumps(v)
                elif not isinstance(v, basestring):
                    obj[k] = MARSHAL_PREFIX + pickle.dumps(v)

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
                self.expressions.append(create_index_expression(column_name,
                                                           value, op))

        for k, v in kwargs.iteritems():
            self.expressions.append(create_index_expression(k, v, index.EQ))

        return self


    def get(self, number=1, key=None):
        if key and isinstance(key, basestring):
            res = self.cf.get(key)

            LOG.info('res is: ')
            LOG.info(res)

            for k, v in res.iteritems():
                if isinstance(v, basestring) and v.startswith(MARSHAL_PREFIX):
                    res[k] = pickle.loads(v[len(MARSHAL_PREFIX):])

            for load in self.loads:
                print 'loading stuff!'
                l = res.get(load)
                if l:
                    res[load] = pickle.loads(l)
                else:
                    res[load] = []

            print res
            return res
        elif self.expressions != []:
            clause = create_index_clause(self.expressions)
            res = self.cf.get_indexed_slices(clause, number)
            for index, item in enumerate(res):

                for k, v in item.iteritems():
                    if v.startswith(MARSHAL_PREFIX):
                        item[k] = pickle.loads(v[len(MARSHAL_PREFIX):])

                for load in self.loads:
                    res[index][load] = pickle.loads(item[load])

            return res
