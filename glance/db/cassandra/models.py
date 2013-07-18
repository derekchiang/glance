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

from glance.openstack.common import timeutils
from glance.openstack.common import uuidutils

from glance.db.pyquery.model import Model

import pickle

# from glance.db import models


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