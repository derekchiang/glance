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
                          BooleanType, CassandraType, LexicalUUIDType
from pycassa.columnfamilymap import ColumnFamilyMap
from pycassa.columnfamily import ColumnFamily
from pycassa.cassandra.ttypes import NotFoundException

from glance.openstack.common import timeutils
from glance.openstack.common import uuidutils

from glance.db.pyquery.model import Model

import pickle

# TODO: set it up
pool = None

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

class Image(object):
    """Represents an image in the datastore"""
    key = LexicalUUIDType()
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

    properties = ArrayType()
    tags = ArrayType()
    locations = ArrayType()
    members = ArrayType()

    def __init__(self):
        self.id = uuidutils.generate_uuid()
        self.created_at = timeutils.utcnow()
        self.updated_at = timeutils.utcnow()

    def to_dict(self):
        d = self.__dict__.copy()
        # NOTE(flaper87): Remove
        # private state instance
        # It is not serializable
        # and causes CircularReference
        d.pop("_sa_instance_state")
        return d

    def save(self):
        cls = self.__class__    
    
        related_cfs = secondary_index_repo.get_related_cf(cls)
        for target_cf, target_field, target_index_field,\
            index_cf, index_field, cf_name in related_cfs:
            
            cf = ColumnFamily(pool, cf_name)
            
            if cls == target_cf:
                for child in getattr(self, target_index_field):
                    row_key = getattr(child, index_field)
                    column_val = getattr(self, target_field)
            elif cls == index_cf:
                for child in getattr(self, target_index_field):
                    row_key = getattr(self, index_field)
                    if (target_field == 'all'):
                        column_val = getattr(self, target_field)
            
            try:
                original_value = cf.get(row_key)
                cf.insert({
                    row_key: dict(original_value, **{column_val: ''})
                    })
            except:
                cf.insert({
                    row_key: {column_val: ''}
                    })


        cfm = ColumnFamilyMap(cls)
        cfm.insert(self)

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

def register_models(engine):
    """
    Creates database tables for all models with the given engine
    """
    models = (Image, ImageProperty, ImageMember)
    for model in models:
        model.metadata.create_all(engine)


def unregister_models(engine):
    """
    Drops database tables for all models with the given engine
    """
    models = (Image, ImageProperty)
    for model in models:
        model.metadata.drop_all(engine)


secondary_index_repo = SecondaryIndexRepo()
secondary_index_repo.add(Image, 'id', 'members', ImageMember, 'member',\
    'image_ids_by_image_member_member')
secondary_index_repo.add(Image, 'id', 'members', ImageMember, 'status',\
    'image_ids_by_image_member_status')
secondary_index_repo.add(Image, 'id', ImageMember, 'deleted',\
    'image_ids_by_image_member_deleted')
secondary_index_repo.add(Image, 'id', ImageProperty, 'name',\
    'image_ids_by_image_property_name')
secondary_index_repo.add(Image, 'id', ImageProperty, 'value',\
    'image_ids_by_image_property_value')
secondary_index_repo.add(Image, 'id', ImageProperty, 'deleted',\
    'image_ids_by_image_property_deleted')
secondary_index_repo.add(Image, 'id', ImageLocation, 'image_id',\
    'image_ids_by_image_location_image_id')
secondary_index_repo.add(Image, 'id', ImageLocation, 'deleted',\
    'image_ids_by_image_location_deleted')
secondary_index_repo.add(ImageMember, 'all', Image, 'owner',\
    'image_members_by_image_owner')
secondary_index_repo.add(ImageTag, 'all', ImageTag, 'image_id',\
    'image_tags_by_image_tag_image_id')
secondary_index_repo.add(ImageTag, 'all', ImageTag, 'deleted',\
    'image_tags_by_image_tag_deleted')