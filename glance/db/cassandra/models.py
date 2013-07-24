# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
SQLAlchemy models for glance data
"""
from pycassa.types import DateType, IntegerType, UTF8Type, \
                          BooleanType, CassandraType
from pycassa.system_manager import SystemManager, SIMPLE_STRATEGY, \
                            UTF8_TYPE, INT_TYPE, BOOLEAN_TYPE, DATE_TYPE

from pycassa.columnfamilymap import ColumnFamilyMap
from pycassa.columnfamily import ColumnFamily
from pycassa.cassandra.ttypes import NotFoundException
from pycassa import ConnectionPool

from glance.openstack.common import timeutils
from glance.openstack.common import uuidutils

from glance.db.pyquery.spec import Attr, EQ
from glance.db.pyquery.model import Model

import pickle

# TODO: set it up
KEYSPACE_NAME = 'GLANCE'
pool = None
sys = None

class SerializableClass(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    @classmethod
    def pack(cls, val):
        return pickle.dumps(val.to_dict())

    @classmethod
    def unpack(cls, strval):
        return cls(pickle.loads(strval))


class SerializableModelBase(SerializableClass):
    """Base class for Nova and Glance Models"""
    is_standalone = False

    def to_dict(self):
        return {
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'deleted_at': self.deleted_at,
            'deleted': self.deleted
        }


class ImageProperty(SerializableModelBase):
    """Represents an image properties in the datastore"""
    def to_dict(self):
        return dict(super(ImageProperty, self).to_dict().items() + \
                    {
                    'id': self.id,
                    'name': self.name,
                    'value': self.value
                    }.items())


class ImageTag(SerializableModelBase):
    """Represents an image tag in the datastore"""
    def to_dict(self):
        return dict(super(ImageTag, self).to_dict().items() + \
                    {
                    'vale': self.value
                    })


class ImageLocation(SerializableModelBase):
    """Represents an image location in the datastore"""
    def to_dict(self):
        return dict(super(ImageLocation, self).to_dict().items() + \
                    {
                    'value': self.value
                    })


class ImageMember(SerializableModelBase):
    def to_dict(self):
        return dict(super(ImageMember, self).to_dict().items() + \
                    {
                    'member': self.member,
                    'can_share': self.can_share,
                    'status': self.status
                    })


class ArrayType(CassandraType):
    @staticmethod
    def pack(arr):
        res = []
        for item in arr:
            res.push(item.pack())
        return pickle.dumps(res)

    @staticmethod
    def unpack(strarr):
        arr = pickle.loads(strarr)
        res = []
        for item in arr:
            res.push(item.unpack())
        return res

    # Just so that this becomes iterable
    def __iter__(self):
        return
        yield


def get_row_key(k, v):
    """Concatenate k and v to get a string suitable to use
    as a row key in Cassandra
    """
    return str(k) + ' = ' + str(v)


class Image(Model):
    """Represents an image in the datastore"""
    cf_name = 'Images'

    key = UTF8Type()
    id = UTF8Type()
    name = UTF8Type()
    disk_format = UTF8Type()
    container_format = UTF8Type()
    size = IntegerType()
    status = UTF8Type()
    is_public = BooleanType(default=False)
    checksum = UTF8Type()
    min_disk = IntegerType(default=0)
    min_ram = IntegerType(default=0)
    owner = UTF8Type()
    protected = BooleanType(default=False)

    created_at = DateType()
    updated_at = DateType()
    deleted_at = DateType()
    deleted = BooleanType(default=False)

    properties = ArrayType(default=[])
    tags = ArrayType(default=[])
    locations = ArrayType(default=[])
    members = ArrayType(default=[])

    def __init__(self, **kwargs):
        self.id = uuidutils.generate_uuid()
        self.created_at = timeutils.utcnow()
        self.updated_at = timeutils.utcnow()

        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    def to_dict(self):
        d = self.__dict__.copy()
        return d

    def save(self, session=None):
        cls = self.__class__

        # Populate related secondary index tables
        for field, cf in secondary_index_repo.get(cls):
            children = getattr(self, field)

            for child in children:
                print child
                d = child.to_dict()
                for k, v in d.iteritems():
                    row_key = get_row_key(k, v)
                    try:
                        original_value = cf.get(row_key)
                        cf.insert({
                            row_key: dict(original_value, **{self.key: ''})
                            })
                    except NotFoundException:
                        cf.insert({
                            row_key: {self.key: ''}
                            })

        cfm = ColumnFamilyMap(cls, pool, cls.cf_name)
        self.key = self.id
        cfm.insert(self)
    
        # related_cfs = secondary_index_repo.get_related_cf(cls)
        # for target_cf, target_field, target_index_field,\
        #     index_cf, index_field, cf_name in related_cfs:
            
        #     cf = ColumnFamily(pool, cf_name)
            
        #     if cls == target_cf:
        #         for child in getattr(self, target_index_field):
        #             row_key = getattr(child, index_field)
        #             column_val = getattr(self, target_field)
        #     elif cls == index_cf:
        #         for child in getattr(self, target_index_field):
        #             row_key = getattr(self, index_field)
        #             if (target_field == 'all'):
        #                 column_val = getattr(self, target_field)
            
        #     try:
        #         original_value = cf.get(row_key)
        #         cf.insert({
        #             row_key: dict(original_value, **{column_val: ''})
        #             })
        #     except:
        #         cf.insert({
        #             row_key: {column_val: ''}
        #             })

        # for child in getattr(self, child_name): 
        #     row_key = getattr(child, row_key_name)
        #     column_val = getattr(self, column_key)
        #     try:
        #         original_value = cf.get(row_key)
        #         cf.insert({
        #             row_key: dict(original_value, **{column_val: ''})
        #             })
        #     except NotFoundException:
        #         cf.insert({
        #             row_key: {column_val: ''}
        #             })

    def delete(self, session=None):
        """Delete this object"""
        self.deleted = True
        self.deleted_at = timeutils.utcnow()
        self.save()

    def update(self, values):
        """dict.update() behaviour."""
        for k, v in values.iteritems():
            setattr(self, k, v)

class SecondaryIndexRepo(object):
    def __init__(self):
        self.repo = []

    def add(self, *args):
        self.repo.append(args)

    def get(self, cls):
        for r in self.repo:
            if r[0] == cls:
                cf_name = r[3]
                cf = ColumnFamily(pool, cf_name)
                yield (r[1], cf)


    def get_data(self, cls, spec):
        assert (isinstance(spec, Attr) and isinstance(spec.value_spec, EQ))
        for r in self.repo:
            if r[2] == cls:
                cf_name = r[3]
                cf = ColumnFamily(pool, cf_name)
                d = cf.get(get_row_key(spec.attr, spec.value_spec.value))
                
                results = []
                for k, v in d.iteritems():
                    results.append(k)
                
                return ColumnFamilyMap(r[0], pool, r[0].cf_name), results, r[1]

        raise Exception('The given class is not defined in this\
                         secondary index repo.')


secondary_index_repo = SecondaryIndexRepo()
secondary_index_repo.add(Image, 'members', ImageMember,\
                         'Images_By_Image_Member')
secondary_index_repo.add(Image, 'properties', ImageProperty,\
                         'Images_By_Image_Property')
secondary_index_repo.add(Image, 'locations', ImageLocation,\
                         'Images_By_Image_Location')


def register_models():
    """
    Creates database tables for all models with the given engine
    """
    global pool, sys
    sys = SystemManager()

    if KEYSPACE_NAME not in sys.list_keyspaces():
        sys.create_keyspace(KEYSPACE_NAME, SIMPLE_STRATEGY, {'replication_factor': '1'})

        image_validators = {
            'id': UTF8Type(),
            'name': UTF8Type(),
            'disk_format': UTF8Type(),
            'container_format': UTF8Type(),
            'size': IntegerType(),
            'status': UTF8Type(),
            'is_public': BooleanType(),
            'checksum': UTF8Type(),
            'min_disk': IntegerType(),
            'min_ram': IntegerType(),
            'owner': UTF8Type(),
            'protected': BooleanType(),
            'created_at': DateType(),
            'updated_at': DateType(),
            'deleted_at': DateType(),
            'deleted': BooleanType()
        }

        sys.create_column_family(KEYSPACE_NAME, 'Images',
                                comparator_type=UTF8Type(),
                                key_validation_class=UTF8Type(), 
                                column_validation_classes=image_validators)

        # Create indices on columns
        sys.create_index(KEYSPACE_NAME, 'Images', 'id', 'UTF8Type')

        sys.create_column_family(KEYSPACE_NAME, 'Images_By_Image_Member')
        sys.create_column_family(KEYSPACE_NAME, 'Images_By_Image_Property')
        sys.create_column_family(KEYSPACE_NAME, 'Images_By_Image_Location')

    pool = ConnectionPool(KEYSPACE_NAME)


def unregister_models():
    """
    Drops database tables for all models with the given engine
    """
    global sys
    sys.drop_keyspace(KEYSPACE_NAME)