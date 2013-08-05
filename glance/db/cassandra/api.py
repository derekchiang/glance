# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# Copyright 2010-2011 OpenStack LLC.
# Copyright 2012 Justin Santa Barbara
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
Defines interface for DB access
"""

import logging
import time
import json
from datetime import datetime

from oslo.config import cfg

from glance.common import exception

import glance.openstack.common.log as os_logging
from glance.openstack.common import timeutils
from glance.openstack.common import uuidutils

from pycassa.pool import ConnectionPool
from pycassa import NotFoundException
from pycassa.system_manager import SystemManager, SIMPLE_STRATEGY
from pycassa.columnfamily import ColumnFamily
from pycassa.index import create_index_expression, create_index_clause
from pycassa import index
from pycassa.batch import Mutator

from glance.db.cassandra.models import register_models, unregister_models
from glance.db.cassandra.spec import EQ, Attr, Any, And, Or, NEQ, Identity, inspect_attr

pool = None
image_cf = None
inverted_indices_cf = None
LOG = os_logging.getLogger(__name__)
KEYSPACE_NAME = 'GLANCE'

STATUSES = ['active', 'saving', 'queued', 'killed', 'pending_delete',
            'deleted']

LOCATION_PREFIX = '__location_'
PROPERTY_PREFIX = '__property_'
TAG_PREFIX = '__tag_'
MEMBER_PREFIX = '__member_'
MARSHAL_PREFIX = '__marshal_'

IMAGE_FIELDS = ['id', 'name', 'disk_format', 'container_format', 'size',
                'status', 'is_public', 'checksum', 'min_disk', 'min_ram',
                'owner', 'protected']
# Fields that are supposed to be datetime
DATETIME_FIELDS = ['created_at', 'updated_at', 'deleted_at']


cassandra_connection_opt = cfg.StrOpt('glance_cassandra_url',
                                default='127.0.0.1:9160',
                                secret=True,
                                metavar='CONNECTION',
                                help=_('A valid Cassandra connection '
                                       'string for the registry database. '
                                       'Default: %(default)s'))

db_opts = [
    cfg.StrOpt('read_consistency_level', default='ALL',
               help=_('The level of read consistency that'
                      'every will be used as the default for every read')),
    cfg.StrOpt('write_consistency_level', default='ALL',
               help=_('The level of write consistency that'
                      'every will be used as the default for every write'))
]

CONF = cfg.CONF
CONF.register_opt(cassandra_connection_opt)
CONF.register_opts(db_opts)
CONF.import_opt('debug', 'glance.openstack.common.log')


trace_enabled = True

def trace(func):
    '''For debug purpose only.
    '''
    def wrapper(*args, **kwargs):
        print func.__name__ 
        return func(*args, **kwargs)

    return wrapper


def add_cli_options():
    """Allows passing sql_connection as a CLI argument."""

    # NOTE(flaper87): Find a better place / way for this.
    CONF.unregister_opt(cassandra_connection_opt)
    CONF.register_cli_opt(cassandra_connection_opt)


def setup_db_env():
    """
    Setup global configuration for database.
    """
    global pool, image_cf, inverted_indices_cf

    sys = SystemManager()
    if KEYSPACE_NAME not in sys.list_keyspaces():
        register_models()

    pool = ConnectionPool(KEYSPACE_NAME,\
                          [CONF.glance_cassandra_url])
    image_cf = ColumnFamily(pool, 'Images')
    inverted_indices_cf = ColumnFamily(pool, 'InvertedIndices')
    # repo = ImageRepo(pool)


def clear_db_env():
    """
    Unset global configuration variables for database.
    """
    global pool

    pool.dispose()

def save_inverted_indices(obj, prefix, batch):
    image_id = obj['image_id']

    # Save all attributes as inverted indices
    for k, v in obj.iteritems():
        # row key would be something like:
        # members.status=pending
        if k != 'image_id':
            row_key = prefix + '.' + str(k) + '=' + str(v)
            batch.insert(inverted_indices_cf, row_key, {image_id: ''})


def delete_inverted_indices(obj, prefix, batch):
    image_id = obj.get('image_id')

    # When a property is just getting created, it has no image_id
    # nor inverted indices associated with it, so we don't need to
    # delete anything
    if image_id:
        for k, v in obj.iteritems():
            if k != 'image_id':
                row_key = prefix + '.' + str(k) + '=' + str(v)
                batch.remove(inverted_indices_cf, row_key, [image_id])


def query_inverted_indices(prefix, key, value):
    row_key = prefix + '.' + str(key) + '=' + str(value)

    return inverted_indices_cf.get(row_key)

# TODO: Some registry APIs make use of the `deleted` attribute,
# so I'm still creating this attribute for backward compatibility,
# even though we don't need soft delete anymore.  Discuss what to do.

def create_image(**kwargs):
    return dict({
        'created_at': timeutils.utcnow(),
        'updated_at': timeutils.utcnow(),
        'id': uuidutils.generate_uuid(),
        'is_public': False,
        'min_disk': 0,
        'min_ram': 0,
        'protected': False,
        'checksum': None,
        'disk_format': None,
        'container_format': None,
        'deleted': False,
        'deleted_at': None,
        'name': None,
        'owner': None,
        'size': None
    }, **kwargs)

def create_image_member(**kwargs):
    return dict({
        'created_at': timeutils.utcnow(),
        'updated_at': timeutils.utcnow(),
        'id': MEMBER_PREFIX + uuidutils.generate_uuid(),
        'can_share': False,
        'status': 'pending',
        'deleted_at': None,
        'deleted': False
    }, **kwargs)

def create_image_location(**kwargs):
    return dict({
        'created_at': timeutils.utcnow(),
        'updated_at': timeutils.utcnow(),
        'id': LOCATION_PREFIX + uuidutils.generate_uuid(),
        'meta_data': {},
        'deleted_at': None,
        'deleted': False
    }, **kwargs)

def create_image_property(**kwargs):
    return dict({
        'created_at': timeutils.utcnow(),
        'updated_at': timeutils.utcnow(),
        'id': PROPERTY_PREFIX + uuidutils.generate_uuid(),
        'deleted_at': None,
        'deleted': False
    }, **kwargs)


def create_image_tag(**kwargs):
    return dict({
        'created_at': timeutils.utcnow(),
        'updated_at': timeutils.utcnow(),
        'id': TAG_PREFIX + uuidutils.generate_uuid(),
        'deleted_at': None,
        'deleted': False
    }, **kwargs)


def dumps(obj):
    return MARSHAL_PREFIX + json.dumps(obj, default=lambda x: time.mktime(x.timetuple()))

def loads(string):
    if string.startswith(MARSHAL_PREFIX):
        obj = json.loads(string[len(MARSHAL_PREFIX):])
    else:
        obj = json.loads(string)

    if isinstance(obj, dict):
        for k, v in obj.iteritems():
            if k in DATETIME_FIELDS:
                if isinstance(obj[k], float):
                    obj[k] = datetime.fromtimestamp(v)

    return obj


def marshal_image(obj):
    output = {}

    for k, v in obj.iteritems():
        if not isinstance(v, basestring):
            output[k] = dumps(v)
        else:
            output[k] = v

    return output


def unmarshal_image(image):
    output = {}
    locations = []
    properties = []
    tags = []
    members = []

    for column, value in image.iteritems():
        if column.startswith(LOCATION_PREFIX):
            # If it's string, it needs to be unmarshalled
            if isinstance(value, basestring):
                value = loads(value) 
            locations.append(value)
        elif column.startswith(PROPERTY_PREFIX):
            if isinstance(value, basestring):
                value = loads(value) 
            properties.append(value)
        elif column.startswith(TAG_PREFIX):
            if isinstance(value, basestring):
                value = loads(value) 
            tags.append(value)
        elif column.startswith(MEMBER_PREFIX):
            if isinstance(value, basestring):
                value = loads(value) 
            members.append(value)
        elif isinstance(value, basestring) and value.startswith(MARSHAL_PREFIX):
            output[column] = loads(value)
        else:
            output[column] = value

    output['locations'] = locations
    output['properties'] = properties
    output['tags'] = tags
    output['members'] = members

    return output


def sort_dicts(dicts, orders):
    """Sort a list of dictionaries"""
    def compare(a, b):
        for field, dir in orders:
            if dir == 'asc':
                if a[field] > b[field]:
                    return 1
                elif a[field] < b[field]:
                    return -1
                else:
                    continue
            elif dir == 'des':
                if a[field] < b[field]:
                    return 1
                elif a[field] > b[field]:
                    return -1
                else:
                    continue
        return 0
    # Some ordering must be given
    assert len(orders) > 0

    # Using insertion sort for simplicity
    out = []
    for d in dicts:
        i = len(out) - 1
        while i >= 0 and compare(out[i], d) > 0:
            i = i - 1
        out.insert(i+1, d)

    return out



def _check_mutate_authorization(context, image):
    if not is_image_mutable(context, image):
        LOG.info(_("Attempted to modify image user did not own."))
        msg = _("You do not own this image")
        if image['is_public']:
            exc_class = exception.ForbiddenPublicImage
        else:
            exc_class = exception.Forbidden

        raise exc_class(msg)


__protected_attributes__ = set([
        "created_at", "updated_at", "deleted_at", "deleted"])


def drop_protected_attrs(vals):
    for attr in __protected_attributes__:
        if attr in vals:
            del vals[attr]
        

@trace
def image_create(context, values):
    """Create an image from the values dictionary."""
    return _image_update(context, values, None, False)

@trace
def image_update(context, image_id, values, purge_props=False):
    """
    Set the given properties on an image and update it.

    :raises NotFound if image does not exist.
    """
    return _image_update(context, values, image_id, purge_props)

@trace
def image_destroy(context, image_id):
    """Destroy the image or raise if it does not exist."""

    image = image_get(context, image_id)

    image_cf.remove(image_id)

    return image

@trace
def _normalize_locations(image):
    undeleted_locations = filter(lambda x: not x['deleted'], image['locations'])
    image['locations'] = [{'url': loc['value'],
                           'metadata': loc['meta_data']}
                          for loc in undeleted_locations]
    return image

@trace
def image_get(context, image_id, session=None, force_show_deleted=False):
    try:
        row = image_cf.get(image_id)
        image = unmarshal_image(row)

        # TODO: Deleting tags for now since the tests are not expecting
        # tags being returned.  Discuss later.
        del image['tags']

    except NotFoundException:
        msg = (_("No image found with ID %s") % image_id)
        LOG.debug(msg)
        raise exception.NotFound(msg)

    # Make sure they can look at it
    if not is_image_visible(context, image):
        msg = (_("Forbidding request, image %s not visible") % image_id)
        LOG.debug(msg)
        raise exception.Forbidden(msg)

    return image

def is_image_mutable(context, image):
    """Return True if the image is mutable in this context."""
    # Is admin == image mutable
    if context.is_admin:
        return True

    # No owner == image not mutable
    if image['owner'] is None or context.owner is None:
        return False

    # Image only mutable by its owner
    return image['owner'] == context.owner


def is_image_sharable(context, image, **kwargs):
    """Return True if the image can be shared to others in this context."""
    # Is admin == image sharable
    if context.is_admin:
        return True

    # Only allow sharing if we have an owner
    if context.owner is None:
        return False

    # If we own the image, we can share it
    if context.owner == image['owner']:
        return True

    # Let's get the membership association
    if 'membership' in kwargs:
        membership = kwargs['membership']
        if membership is None:
            # Not shared with us anyway
            return False
    else:
        members = image_member_find(context,
                                    image_id=image['id'],
                                    member=context.owner)
        if members:
            member = members[0]
        else:
            # Not shared with us anyway
            return False

    # It's the can_share attribute we're now interested in
    return member['can_share']


def is_image_visible(context, image, status=None):
    """Return True if the image is visible in this context."""
    # Is admin == image visible
    if context.is_admin:
        return True

    # No owner == image visible
    if image['owner'] is None:
        return True

    # Image is_public == image visible
    if image['is_public']:
        return True

    # Perform tests based on whether we have an owner
    if context.owner is not None:
        if context.owner == image['owner']:
            return True

        # Figure out if this image is shared with that tenant
        members = image_member_find(context,
                                    image_id=image['id'],
                                    member=context.owner,
                                    status=status)
        if members:
            return True

    # Private image
    return False


def _paginate_query(query, model, limit, sort_keys, marker=None,
                    sort_dir=None, sort_dirs=None):
    """Returns a query with sorting / pagination criteria added.

    Pagination works by requiring a unique sort_key, specified by sort_keys.
    (If sort_keys is not unique, then we risk looping through values.)
    We use the last row in the previous page as the 'marker' for pagination.
    So we must return values that follow the passed marker in the order.
    With a single-valued sort_key, this would be easy: sort_key > X.
    With a compound-values sort_key, (k1, k2, k3) we must do this to repeat
    the lexicographical ordering:
    (k1 > X1) or (k1 == X1 && k2 > X2) or (k1 == X1 && k2 == X2 && k3 > X3)

    We also have to cope with different sort_directions.

    Typically, the id of the last row is used as the client-facing pagination
    marker, then the actual marker object must be fetched from the db and
    passed in to us as marker.

    :param query: the query object to which we should add paging/sorting
    :param model: the ORM model class
    :param limit: maximum number of items to return
    :param sort_keys: array of attributes by which results should be sorted
    :param marker: the last item of the previous page; we returns the next
                    results after this value.
    :param sort_dir: direction in which results should be sorted (asc, desc)
    :param sort_dirs: per-column array of sort_dirs, corresponding to sort_keys

    :rtype: sqlalchemy.orm.query.Query
    :return: The query with sorting/pagination added.
    """

    if 'id' not in sort_keys:
        # TODO(justinsb): If this ever gives a false-positive, check
        # the actual primary key, rather than assuming its id
        LOG.warn(_('Id not in sort_keys; is sort_keys unique?'))

    assert(not (sort_dir and sort_dirs))

    # Default the sort direction to ascending
    if sort_dirs is None and sort_dir is None:
        sort_dir = 'asc'

    # Ensure a per-column sort direction
    if sort_dirs is None:
        sort_dirs = [sort_dir for _sort_key in sort_keys]

    assert(len(sort_dirs) == len(sort_keys))

    # Add sorting
    for current_sort_key, current_sort_dir in zip(sort_keys, sort_dirs):
        sort_dir_func = {
            'asc': sqlalchemy.asc,
            'desc': sqlalchemy.desc,
        }[current_sort_dir]

        try:
            sort_key_attr = getattr(model, current_sort_key)
        except AttributeError:
            raise exception.InvalidSortKey()
        query = query.order_by(sort_dir_func(sort_key_attr))

    default = ''  # Default to an empty string if NULL

    # Add pagination
    if marker is not None:
        marker_values = []
        for sort_key in sort_keys:
            v = getattr(marker, sort_key)
            if v is None:
                v = default
            marker_values.append(v)

        # Build up an array of sort criteria as in the docstring
        criteria_list = []
        for i in xrange(0, len(sort_keys)):
            crit_attrs = []
            for j in xrange(0, i):
                model_attr = getattr(model, sort_keys[j])
                attr = sa_sql.expression.case([(model_attr != None,
                                              model_attr), ],
                                              else_=default)
                crit_attrs.append((attr == marker_values[j]))

            model_attr = getattr(model, sort_keys[i])
            attr = sa_sql.expression.case([(model_attr != None,
                                          model_attr), ],
                                          else_=default)
            if sort_dirs[i] == 'desc':
                crit_attrs.append((attr < marker_values[i]))
            elif sort_dirs[i] == 'asc':
                crit_attrs.append((attr > marker_values[i]))
            else:
                raise ValueError(_("Unknown sort direction, "
                                   "must be 'desc' or 'asc'"))

            criteria = sa_sql.and_(*crit_attrs)
            criteria_list.append(criteria)

        f = sa_sql.or_(*criteria_list)
        query = query.filter(f)

    if limit is not None:
        query = query.limit(limit)

    return query

@trace
def image_get_all(context, filters=None, marker=None, limit=None,
                  sort_key='created_at', sort_dir='desc',
                  member_status='accepted', is_public=None,
                  admin_as_user=False):
    """
    Get all images that match zero or more filters.

    :param filters: dict of filter keys and values. If a 'properties'
                    key is present, it is treated as a dict of key/value
                    filters on the image properties attribute
    :param marker: image id after which to start page
    :param limit: maximum number of images to return
    :param sort_key: image attribute by which results should be sorted
    :param sort_dir: direction in which results should be sorted (asc, desc)
    :param member_status: only return shared images that have this membership
                          status
    :param is_public: If true, return only public images. If false, return
                      only private and shared images.
    :param admin_as_user: For backwards compatibility. If true, then return to
                      an admin the equivalent set of images which it would see
                      if it were a regular user
    """

    filters = filters or {}
    limit = limit or 99999999

    print 'the arguments are:'
    print context.is_admin
    print context.owner
    print filters
    print marker
    print limit
    print sort_key
    print sort_dir
    print member_status
    print is_public
    print admin_as_user

    # We are querying for images that satisfy one of the following conditions:
    # 1. Anyone can see it (is_public = True)
    # 2. I own it (owner = context.owner)
    # 3. Someone has shared it with me (ImageMember.member = context.owner)

    # To query for the first two conditions, we ask Cassandra directly.
    # To query for the last condition, we use inverted indices to query for
    # all images whose member is context.owner, then we use client-side
    # filters.

    public_image_filters = []
    own_image_filters = []
    shared_image_ids = []

    client_side_filters = []

    if (not context.is_admin) or admin_as_user == True:
        public_image_filters.append(create_index_expression('is_public', True, index.EQ))

        if context.owner is not None:
            own_image_filters.append(create_index_expression('owner', context.owner, index.EQ))

            shared_image_ids = set(query_inverted_indices(
                MEMBER_PREFIX, 'member', context.owner))

            if member_status != 'all':
                shared_image_ids = shared_image_ids.union(
                    query_inverted_indices(MEMBER_PREFIX, 'status', member_status))

    common_filters = []

    if 'visibility' in filters:
        visibility = filters.pop('visibility')
        if visibility == 'public':
            common_filters.append(create_index_expression('is_public', True, index.EQ))
        elif visibility == 'private':
            common_filters.append(create_index_expression('is_public', False, index.EQ))
            if context.owner is not None and ((not context.is_admin)
                                              or admin_as_user == True):
                common_filters.append('owner', context.owner, index.EQ)
        else:
            shared_image_ids.union(query_inverted_indices(
                MEMBER_PREFIX, 'member', context.owner))
            public_image_filters = []
            own_image_filters = []

    if is_public is not None:
        common_filters.append(create_index_expression('is_public', is_public, index.EQ))

    if 'is_public' in filters:
        client_side_filters.append(Attr('properties',
                                   Any(And(Attr('name', EQ('is_public')),
                                           Attr('value',
                                                EQ(filters.pop('is_public'))),
                                           Attr('deleted', EQ(False))))))

    if 'checksum' in filters:
        checksum = filters.get('checksum')
        common_filters.append(create_index_expression('checksum', checksum, index.EQ))

    if 'changes-since' in filters:
        # normalize timestamp to UTC, as sqlalchemy doesn't appear to
        # respect timezone offsets
        changes_since = timeutils.normalize_time(filters.pop('changes-since'))
        common_filters.append(create_index_expression('updated_at', changes_since, index.GT))


    if 'deleted' in filters:
        deleted_filter = filters.pop('deleted')
        if not deleted_filter:
            # Cassandra doesn't support NEQ, so we handle it on the client side
            client_side_filters.append(Attr('status', NEQ('killed')))


    for k, v in filters.pop('properties', {}).iteritems():
        client_side_filters.append(Attr('properties',
                                   Any(And(Attr('name', EQ(k)),
                                           Attr('value',
                                                EQ(v)),
                                           Attr('deleted', EQ(False))))))

    for k, v in filters.iteritems():
        if v is not None:
            key = k
            if k.endswith('_min') or k.endswith('_max'):
                key = key[0:-4]
                try:
                    v = int(v)
                except ValueError:
                    msg = _("Unable to filter on a range "
                            "with a non-numeric value.")
                    raise exception.InvalidFilterRangeValue(msg)

            if k.endswith('_min'):
                common_filters.append(create_index_expression(key, v, index.GTE))
            elif k.endswith('_max'):
                common_filters.append(create_index_expression(key, v, index.LTE))
            elif key in IMAGE_FIELDS:
                common_filters.append(create_index_expression(key, v, index.EQ))
            else:
                client_side_filters.append(Attr('properties',
                                           Any(And(Attr('name', EQ(key)),
                                                   Attr('value',
                                                        EQ(v))))))

    print 'wakakaka'
    print public_image_filters
    print own_image_filters
    print shared_image_ids
    print client_side_filters
    print common_filters

    client_side_filters = And(*client_side_filters)

    images = []

    # To avoid repeated images
    key_set = set()

    if public_image_filters != []:
        public_image_filters.extend(common_filters)
        res = image_cf.get_indexed_slices(create_index_clause(
            public_image_filters, count=limit))
        for key, image in res:
            if key not in key_set:
                images.append(image)
                key_set.add(key)

    if own_image_filters != []:
        own_image_filters.extend(common_filters)
        res = image_cf.get_indexed_slices(create_index_clause(
            own_image_filters, count=limit))
        for key, image in res:
            if key not in key_set:
                images.append(image)
                key_set.add(key)

    if shared_image_ids != []:
        for image_id in shared_image_ids:
            temp_filters = [create_index_expression('id', image_id, index.EQ)]
            temp_filters.extend(common_filters)
            res = image_cf.get_indexed_slices(create_index_clause(
                temp_filters, count=limit))
            for key, image in res:
                if key not in key_set:
                    images.append(image)
                    key_set.add(key)

    images = [unmarshal_image(image) for image in images]
    images = filter(lambda x: client_side_filters.match(x), images)

    print images

    # TODO: pagination

    return images

@trace
def _validate_image(values):
    """
    Validates the incoming data and raises a Invalid exception
    if anything is out of order.

    :param values: Mapping of image metadata to check
    """
    status = values.get('status', None)
    if not status:
        msg = "Image status is required."
        raise exception.Invalid(msg)

    if status not in STATUSES:
        msg = "Invalid image status '%s' for image." % status
        raise exception.Invalid(msg)

    return values


def _update_values(image_ref, values):
    for k in values:
        if getattr(image_ref, k) != values[k]:
            setattr(image_ref, k, values[k])

@trace
def _image_update(context, values, image_id, purge_props=False):
    """
    Used internally by image_create and image_update

    :param context: Request context
    :param values: A dict of attributes to set
    :param image_id: If None, create the image, otherwise, find and update it
    """

    #NOTE(jbresnah) values is altered in this so a copy is needed
    values = values.copy()

    LOG.info('real values: ')
    LOG.info(values)

    # Remove the properties passed in the values mapping. We
    # handle properties separately from base image attributes,
    # and leaving properties in the values mapping will cause
    # a SQLAlchemy model error because SQLAlchemy expects the
    # properties attribute of an Image model to be a list and
    # not a dict.
    properties = values.pop('properties', {})

    location_data = values.pop('locations', None)

    if image_id:
        image = image_get(context, image_id)

        # Perform authorization check
        _check_mutate_authorization(context, image)
    else:
        # Test if an image with this id already exists
        # TODO: is there a better way?
        if values.get('id'):
            try:
                # Just checking for existence, so only getting
                # the id column
                _ = image_get(context, values['id'], ['id'])
                raise exception.Duplicate("Image ID %s already exists!"
                                          % values['id'])
            except exception.NotFound:
                pass

        if values.get('size') is not None:
            values['size'] = int(values['size'])

        if 'min_ram' in values:
            values['min_ram'] = int(values['min_ram'] or 0)

        if 'min_disk' in values:
            values['min_disk'] = int(values['min_disk'] or 0)

        values['is_public'] = bool(values.get('is_public', False))
        values['protected'] = bool(values.get('protected', False))

        image = create_image()

    # Need to canonicalize ownership
    if 'owner' in values and not values['owner']:
        values['owner'] = None

    if image_id:
        # Don't drop created_at if we're passing it in...
        drop_protected_attrs(values)
        #NOTE(iccha-sethi): updated_at must be explicitly set in case
        #                   only ImageProperty table was modifited
        values['updated_at'] = timeutils.utcnow()

    # Validate the attributes before we go any further. From my
    # investigation, the @validates decorator does not validate
    # on new records, only on existing records, which is, well,
    # idiotic.
    image = dict(image, **values)
    values = _validate_image(image)

    # Batch operations on inverted indices and images
    batch = Mutator(pool)

    orig_properties = unmarshal_image(image)['properties']
    orig_properties_names = filter(lambda x: x['name'], orig_properties)
    new_properties_names = properties.keys()

    if purge_props:
        prop_names_to_delete = set(orig_properties_names).difference(
                                set(new_properties_names))
        _image_property_delete(context, prop_names_to_delete,
                           image['id'], batch=batch)

    for name in properties:
        prop_values = create_image_property(**{
            'image_id': image['id'],
            'name': name,
            'value': properties[name]
        })

        if name in orig_properties_names:
            _image_property_delete(context, [name], image['id'], batch)

        image_property_create(context, prop_values, batch=batch)
        image[prop_values['id']] = prop_values

    if location_data is not None:
        _image_locations_set(image, location_data, batch)

    batch.insert(image_cf, image['id'], marshal_image(image))

    batch.send()

    image = unmarshal_image(image)
    # TODO: Deleting tags for now since the tests are not expecting
    # tags being returned.  Discuss later.
    del image['tags']
    return image

@trace
def _image_locations_set(image, locations, batch=None):

    # Remove existing locations
    remove_columns = []
    for column, value in image.iteritems():
        if column.startswith(LOCATION_PREFIX):
            delete_inverted_indices(value, LOCATION_PREFIX, batch)
            # Not directly deleting because the size of image can't
            # change during the iteration
            remove_columns.append(column)

    for column in remove_columns:
        del image[column]

    # TODO: As it currently stands, if there are N locations given,
    # then we will issue N queries to Cassandra, each inserting one
    # location.  But in fact, we could totally insert all locations
    # at once.  We might want to optimize this.

    for location in locations:
        new_location = create_image_location(
                              image_id=image['id'],
                              url=location['url'],
                              metadata=location['metadata'])
        save_inverted_indices(new_location, LOCATION_PREFIX, batch)
        image[new_location['id']] = new_location


def image_property_create(context, values, batch=None):
    """Create an ImageProperty object"""
    prop = create_image_property()
    prop = _image_property_update(context, prop, values,
                                  batch=batch)
    return prop


def _image_property_update(context, prop, values, batch=None):
    """
    Used internally by image_property_create and image_property_update
    """
    using_own_batch = False
    if batch == None:
        using_own_batch = True
        batch = Mutator(pool)

    # TODO: think about whether we need this line
    # drop_protected_attrs(values)

    delete_inverted_indices(prop, PROPERTY_PREFIX, batch)
    prop = dict(prop, **values)
    save_inverted_indices(prop, PROPERTY_PREFIX, batch)

    batch.insert(image_cf, prop['image_id'], {prop['id']: dumps(prop)})

    if using_own_batch:
        batch.send()

    return prop


def image_property_delete(context, prop_name, image_id):
    """
    Used internally by image_property_create and image_property_update
    """
    _image_property_delete(context, [prop_name], image_id)

def _image_property_delete(context, prop_names, image_id, batch=None):
    remove_columns = []

    if prop_names == []:
        return

    image = image_cf.get(image_id)
    for column, value in image.iteritems():
        if column.startswith(PROPERTY_PREFIX):
            value = loads(value)  
            if value['name'] in prop_names:
                delete_inverted_indices(value, PROPERTY_PREFIX, batch)
                remove_columns.append(column)

    if batch:
        batch.remove(image_cf, image_id, remove_columns)
    else:
        image_cf.remove(image_id, remove_columns)


def image_member_create(context, values, session=None):
    """Create an ImageMember object"""
    memb = create_image_member()
    memb = _image_member_update(context, memb, values)
    return memb


def image_member_update(context, memb_id, values):
    """Update an ImageMember object"""

    memb = _image_member_get(context, memb_id)
    return _image_member_update(context, memb, values, session)


def _image_member_update(context, memb, values):
    """Apply supplied dictionary of values to a Member object."""
    batch = Mutator(pool)

    drop_protected_attrs(values)
    values["deleted"] = False
    values.setdefault('can_share', False)

    delete_inverted_indices(memb, MEMBER_PREFIX, batch)

    memb = dict(memb, **values)

    save_inverted_indices(memb, MEMBER_PREFIX, batch)

    batch.insert(image_cf, memb['image_id'], {
        memb['id']: dumps(memb)
    })

    batch.send()

    return memb


def image_member_delete(context, memb_id, session=None):
    """Delete an ImageMember object"""
    member = _image_member_get(context, memb_id)
    delete_inverted_indices(memb)
    image_cf.remove(memb['image_id'], [memb['id']])


def _image_member_get(context, memb_id):
    """Fetch an ImageMember entity by id"""

    image_ids = query_inverted_indices(MEMBER_PREFIX, 'id', memb_id)

    # TODO: is this reasonable?  Can a member belong to multiple images?
    image_id = image_ids[0]

    image = image_cf.get(image_id)
    column_key = memb_id

    return loads(image[column_key])


def image_member_find(context, image_id=None,
                       member=None, status=None):
    """Find all members that meet the given criteria

    :param image_id: identifier of image entity
    :param member: tenant to which membership has been granted
    """

    def construct_criteria_from_image(image):
        if image.get('owner') == context.owner:
            image_is_owner = True
        else:
            image_is_owner = False

        criteria = [Or(Identity(image_is_owner, EQ(True)),
                       Attr('member', EQ(context.owner)))]

        if member is not None:
            criteria.append(Attr('member', EQ(member)))

        if status is not None:
            criteria.append(Attr('status', EQ(status)))

        criteria = And(*criteria)
        return criteria

    def find_matching_members(image, criteria):
        members = []
        for column, value in image.iteritems():
            if column.startswith(MEMBER_PREFIX):
                m = loads(value)
                if criteria.match(m):
                    members.append(m)

        return members

    if image_id:
        image = image_cf.get(image_id)

        criteria = construct_criteria_from_image(image)
    
        return find_matching_members(image, criteria)

    else:
        image_ids = set()
        if member is not None:
            image_ids = image_ids.union(
                query_inverted_indices(MEMBER_PREFIX, 'member', member))

        if status is not None:
            image_ids = image_ids.union(
                query_inverted_indices(MEMBER_PREFIX, 'status', status))

        members = []
        for image_id in image_ids:
            image = image_cf.get(image_id)
            criteria = construct_criteria_from_image(image)
            members.extend(find_matching_members(image, criteria))

        return members


# pylint: disable-msg=C0111
def _can_show_deleted(context):
    """
    Calculates whether to include deleted objects based on context.
    Currently just looks for a flag called deleted in the context dict.
    """
    if hasattr(context, 'show_deleted'):
        return context.show_deleted
    if not hasattr(context, 'get'):
        return False
    return context.get('deleted', False)


def image_tag_set_all(context, image_id, tags):
    existing_tags = set(image_tag_get_all(context, image_id))
    tags = set(tags)

    tags_to_create = tags - existing_tags
    #NOTE(bcwaldon): we call 'reversed' here to ensure the ImageTag.id fields
    # will be populated in the order required to reflect the correct ordering
    # on a subsequent call to image_tag_get_all
    _image_tag_create(context, image_id, reversed(list(tags_to_create)))
    _image_tag_delete(context, image_id, tags_to_delete)


def image_tag_create(context, image_id, value):
    """Create an image tag."""
    _image_tag_create(context, image_id, [value])

    return value

def _image_tag_create(context, image_id, values):
    """Create several tags at once"""
    batch = Mutator(pool)

    tags = {}

    for value in values:
        tag = create_image_tag(image_id=image_id, value=value)
        save_inverted_indices(tag, TAG_PREFIX, batch)
        tags[tag['id']] = dumps(tag)

    print 'tags are:'
    print tags
    batch.insert(image_cf, image_id, tags)

    batch.send()

def image_tag_delete(context, image_id, value):
    """Delete an image tag."""
    _image_tag_delete(context, image_id, [value])

def _image_tag_delete(context, image_id, values):
    """Delete multiple image tags."""
    batch = Mutator(pool)

    columns_to_remove = []

    for value in values:
        tags = unmarshal_image(image.cf.get(image_id))['tags']
        tags = filter(lambda x: x['value'] == value, tags)

        if len(tags) == 0:
            raise exception.NotFound()
        else:
            for tag in tags:
                delete_inverted_indices(tag, TAG_PREFIX, batch)
                columns_to_remove.append(tag['id'])
            
    batch.remove(image_cf, image_id, columns_to_remove)
    batch.send()

def image_tag_get_all(context, image_id):
    """Get a list of tags for a specific image."""
    image = unmarshal_image(image_cf.get(image_id))
    print image
    tags = unmarshal_image(image_cf.get(image_id)).get('tags') or []
    tags = sort_dicts(tags, [('created_at', 'asc')])

    return [tag['value'] for tag in tags]