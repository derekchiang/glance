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
from pycassa import InvalidRequestException
from pycassa.types import DateType, IntegerType, UTF8Type, \
                          BooleanType, CassandraType
from pycassa.system_manager import SystemManager, SIMPLE_STRATEGY, \
                            UTF8_TYPE, INT_TYPE, BOOLEAN_TYPE, DATE_TYPE

KEYSPACE_NAME = 'GLANCE'

def register_models():
    """
    Creates database tables for all models with the given engine
    """
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
            # We are setting 'deleted_at' as a UTF8Type, because
            # we don't really need deleted_at and it's here only
            # for backward capatibility.  It's value is always
            # None, so it can't be stored in Cassandra as DateType
            'deleted_at': UTF8Type(),
            'deleted': BooleanType()
        }

        sys.create_column_family(KEYSPACE_NAME, 'Images',
                                 comparator_type=UTF8Type(),
                                 key_validation_class=UTF8Type(), 
                                 column_validation_classes=image_validators)

        # Create indices on columns
        sys.create_index(KEYSPACE_NAME, 'Images', 'id', 'UTF8Type')
        sys.create_index(KEYSPACE_NAME, 'Images', 'is_public', 'BooleanType')
        sys.create_index(KEYSPACE_NAME, 'Images', 'owner', 'UTF8Type')
        sys.create_index(KEYSPACE_NAME, 'Images', 'name', 'UTF8Type')
        sys.create_index(KEYSPACE_NAME, 'Images', 'checksum', 'UTF8Type')

        sys.create_column_family(KEYSPACE_NAME, 'InvertedIndices',
                                 comparator_type=UTF8Type(),
                                 key_validation_class=UTF8Type())


def unregister_models():
    """
    Drops database tables for all models with the given engine
    """
    sys = SystemManager()

    try:
        sys.drop_column_family(KEYSPACE_NAME, 'Images')
    except InvalidRequestException:
        pass

    try:
        sys.drop_column_family(KEYSPACE_NAME, 'InvertedIndices')
    except InvalidRequestException:
        pass

    try:
        sys.drop_keyspace(KEYSPACE_NAME)
    except InvalidRequestException:
        pass

    sys.close()