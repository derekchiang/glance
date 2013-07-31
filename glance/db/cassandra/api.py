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

from glance.db.cassandra.models import register_models, unregister_models
from glance.db.cassandra.lib import ImageRepo, \
                                    ImageIdDuplicateException, \
                                    ImageIdNotFoundException, \
                                    Models, \
                                    merge_dict, drop_protected_attrs,\
                                    sort_dicts
from glance.db.cassandra.spec import EQ, Attr, Any, And, Or, NEQ

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
MARSHAL_PREFIX = '__'

IMAGE_FIELDS = ['id', 'name', 'disk_format', 'container_format', 'size',
                'status', 'is_public', 'checksum', 'min_disk', 'min_ram',
                'owner', 'protected']
# Fields that are supposed to be datetime
DATETIME_FIELDS = ['created_at', 'updated_at', 'deleted_at']


cassandra_connection_opt = cfg.StrOpt('glance_cassandra_url',
                                default='127.0.0.1:9160',
                                secret=True,
                                metavar='CONNECTION',
                                help=_('A valid SQLAlchemy connection '
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


def get_prefix(model):
    return {
        Models.ImageMember: 'members',
        Models.ImageLocation: 'locations',
        Models.ImageProperty: 'properties',
        Models.ImageTag: 'tags'
    }.get(model)

def save_inverted_indices(obj, model):
    
    prefix = get_prefix(model)

    image_id = obj['image_id']

    # Save all attributes as inverted indices
    for k, v in obj.iteritems():
        # row key would be something like:
        # members.status=pending
        if k != 'image_id':
            row_key = prefix + '.' + str(k) + '=' + str(v)
            inverted_indices_cf.insert(row_key, {image_id: ''})


def delete_inverted_indices(obj, model):

    prefix = get_prefix(model)

    image_id = obj['image_id']

    for k, v in obj.iteritems():
        if k != 'image_id':
            row_key = prefix + '.' + str(k) + '=' + str(v)
            inverted_indices_cf.remove(row_key, [image_id])


def query_inverted_indices(model, key, value):
    
    prefix = get_prefix(model)
    row_key = prefix + '.' + str(key) + '=' + str(value)

    return inverted_indices_cf.get(row_key)


def create(model, **kwargs):
    # Create the given model
    base = merge_dict({
        'created_at': timeutils.utcnow(),
        'updated_at': timeutils.utcnow(),
    }, kwargs)

    if model == Models.Image:
        return merge_dict(base, {
            'id': uuidutils.generate_uuid(),
            'is_public': False,
            'min_disk': 0,
            'min_ram': 0,
            'protected': False,
            'checksum': None,
            'disk_format': None,
            'container_format': None
        })

    elif model == Models.ImageMember:
        return merge_dict(base, {
            'id': MEMBER_PREFIX + uuidutils.generate_uuid(),
            'can_share': False,
            'status': 'pending'
        })

    elif model == Models.ImageLocation:
        return merge_dict(base, {
            'id': LOCATION_PREFIX + uuidutils.generate_uuid(),
            'meta_data': {}
        })

    elif model == Models.ImageProperty:
        return merge_dict(base, {
            'id': PROPERTY_PREFIX + uuidutils.generate_uuid()
        })

    elif model == Models.ImageTag:
        return merge_dict(base, {
            'id': TAG_PREFIX + uuidutils.generate_uuid()
        })

    else:
        raise Exception()

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
                obj[k] = datetime.fromtimestamp(v)

    return obj


def marshal_image(obj):
    obj = obj.copy()
    for k, v in obj.iteritems():
        if not isinstance(v, basestring):
            obj[k] = dumps(v)

    return obj


def unmarshal_image(obj):
    obj = obj.copy()

    for k, v in obj.iteritems():
        if isinstance(v, basestring):
            if v.startswith(MARSHAL_PREFIX):
                obj[k] = loads(v)

    return obj


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

    image = _image_get(context, image_id)

    image_cf.remove(image_id)

    return _transform_image(image)

@trace
def _normalize_locations(image):
    undeleted_locations = filter(lambda x: not x['deleted'], image['locations'])
    image['locations'] = [{'url': loc['value'],
                           'metadata': loc['meta_data']}
                          for loc in undeleted_locations]
    return image

@trace
def image_get(context, image_id, session=None, force_show_deleted=False):
    image = _image_get(context, image_id,
                       force_show_deleted=force_show_deleted)
    image = _transform_image(image)
    return image

def _transform_image(image):
    locations = []
    properties = []
    tags = []
    members = []
    transformed_image = {}
    for column, value in image.iteritems():
        if column.startswith(LOCATION_PREFIX):
            # If it's string, it needs to be unmarshalled
            if isinstance(value, basestring):
                value = loads(value) 
            locations.append(value)
        elif column.startswith('__property'):
            if isinstance(value, basestring):
                value = loads(value) 
            properties.append(value)
        elif column.startswith('__tag'):
            if isinstance(value, basestring):
                value = loads(value) 
            tags.append(value)
        elif column.startswith('__member'):
            if isinstance(value, basestring):
                value = loads(value) 
            members.append(value)
        else:
            transformed_image[column] = value

    transformed_image['locations'] = locations
    transformed_image['properties'] = properties
    transformed_image['tags'] = tags
    transformed_image['members'] = members


    print "transformed image: "
    print transformed_image
    return transformed_image

@trace
def _image_get(context, image_id, force_show_deleted=False):
    """Get an image or raise if it does not exist."""
    try:
        image = image_cf.get(image_id)
        image = unmarshal_image(image)

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

    image_filters = []
    other_filters = []

    if (not context.is_admin) or admin_as_user == True:
        image_filters.append(create_index_expression('is_public', True, index.EQ))
        other_filters.append(Attr('members.deleted', EQ(False)))

        if context.owner is not None:
            image_filters.append(create_index_expression('owner', context.owner, index.EQ))
            other_filters.append(Attr('members.member',
                                      EQ(context, context.owner)))
            if member_status != 'all':
                other_filters.append(Attr('members.status',
                                          EQ(member_status)))

        other_filters = [And(*other_filters)]

    if 'visibility' in filters:
        visibility = filters.pop('visibility')
        if visibility == 'public':
            image_filters.append(create_index_expression('is_public', True, index.EQ))
        elif visibility == 'private':
            image_filters.append(create_index_expression('is_public', False, index.EQ))
            if context.owner is not None and ((not context.is_admin)
                                              or admin_as_user == True):
                image_filters.append('owner', context.owner, index.EQ)
        else:
            other_filters.extend([Attr('members.member', EQ(context.owner)),
                                  Attr('members.deleted', EQ(False))])
            # TODO: exactly why are we doing this?
            image_filters = []

    if is_public is not None:
        image_filters.append(create_index_expression('is_public', is_public, index.EQ))

    if 'is_public' in filters:
        other_filters.append(Attr('properties',
                                  Any(And(Attr('name', EQ('is_public')),
                                          Attr('value',
                                               EQ(filters.pop('is_public'))),
                                          Attr('deleted', EQ(False))))))

    if 'checksum' in filters:
        checksum = filters.get('checksum')
        image_filters.append(create_index_expression('checksum', checksum, index.EQ))

    if 'changes-since' in filters:
        # normalize timestamp to UTC, as sqlalchemy doesn't appear to
        # respect timezone offsets
        changes_since = timeutils.normalize_time(filters.pop('changes-since'))
        image_filters.append(create_index_expression('updated_at', changes_since, index.GT))

    other_filters.append(Attr('status', NEQ('killed')))


    for k, v in filters.pop('properties', {}).iteritems():
        other_filters.append(Attr('properties',
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
                image_filters.append(create_index_expression(key, v, index.GTE))
            elif k.endswith('_max'):
                image_filters.append(create_index_expression(key, v, index.LTE))
            elif key in IMAGE_FIELDS:
                image_filters.append(create_index_expression(key, v, index.EQ))
            else:
                other_filters.append(Attr('properties',
                                          Any(And(Attr('name', EQ(key)),
                                                  Attr('value',
                                                       EQ(v))))))

    # TODO: Setting count to a large number to get all images; not sure if there is a more
    # elegant way
    images = image_cf.get_indexed_slices(create_index_clause(image_filters, count=99999999))
    other_filters = And(*other_filters)
    images = filter(lambda x: other_filters.match(x), images)

    # TODO: pagination

    return [_transform_image(image) for image in images]

@trace
def _validate_image(values):
    """
    Validates the incoming data and raises a Invalid exception
    if anything is out of order.

    :param values: Mapping of image metadata to check
    """
    status = values.get('status')

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
        image = _image_get(context, image_id)

        image_cf.remove(image_id)
        # Perform authorization check
        _check_mutate_authorization(context, image)
    else:
        if values.get('size') is not None:
            values['size'] = int(values['size'])

        if 'min_ram' in values:
            values['min_ram'] = int(values['min_ram'] or 0)

        if 'min_disk' in values:
            values['min_disk'] = int(values['min_disk'] or 0)

        values['is_public'] = bool(values.get('is_public', False))
        values['protected'] = bool(values.get('protected', False))

        image = create(Models.Image)

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
    image = merge_dict(image, values)
    values = _validate_image(image)

    _set_properties_for_image(context, image, properties, purge_props)

    if location_data is not None:
        _image_locations_set(image, location_data)

    # TODO: No exception will be raised if the given key
    # already exists.  Do we need to worry about overriding
    # stuff anyway?
    image_cf.insert(image['id'], marshal_image(image))

    print 'image is: '
    print image

    return _transform_image(image)

@trace
def _image_locations_set(image, locations):

    # Remove existing locations
    for column, value in image.iteritems():
        if column.startswith(LOCATION_PREFIX):
            delete_inverted_indices(value, Models.ImageLocation)
            del image[column]

    # TODO: As it currently stands, if there are N locations given,
    # then we will issue N queries to Cassandra, each inserting one
    # location.  But in fact, we could totally insert all locations
    # at once.  We might want to optimize this.

    for location in locations:
        new_location = create(Models.ImageLocation,
                              image_id=image['id'],
                              url=location['url'],
                              metadata=location['metadata'])
        save_inverted_indices(new_location, Models.ImageLocation)
        image[new_location['id']] = new_location


def _set_properties_for_image(context, image, properties,
                              purge_props=False):
    """
    Create or update a set of image_properties for a given image

    :param context: Request context
    :param image: An Image dictionary
    :param properties: A dict of properties to set
    :param session: A SQLAlchemy session to use (if present)
    """
    orig_properties = {}
    for prop in (image.get('properties') or []):
        orig_properties[prop['name']] = prop

    for name, value in properties.iteritems():
        prop_values = {'image_id': image['id'],
                       'name': name,
                       'value': value}
        if name in orig_properties:
            prop = orig_properties[name]
            _image_property_update(context, prop, prop_values)
        else:
            image_property_create(context, prop_values)

    if purge_props:
        for key in orig_properties.keys():
            if key not in properties:
                prop = orig_properties[key]
                image_property_delete(context, prop.name,
                                      image.id)


def image_property_create(context, values):
    """Create an ImageProperty object"""
    prop = create(Models.ImageProperty)
    prop = _image_property_update(context, prop, values)
    return prop


def _image_property_update(context, prop, values):
    """
    Used internally by image_property_create and image_property_update
    """
    drop_protected_attrs(values)

    prop = merge_dict(prop, values)
    save_inverted_indices(prop, Models.ImageProperty)
    uuid = PROPERTY_PREFIX + uuidutils.generate_uuid()

    image_cf.insert(prop['image_id'], {uuid: (dumps(prop))})

    return prop


def image_property_delete(context, prop_name, image_id, session=None):
    """
    Used internally by image_property_create and image_property_update
    """
    remove_columns = []

    image = image_cf.get(image_id)
    for column, value in image.iteritems():
        if column.startswith(PROPERTY_PREFIX):
            value = loads(value)  
            if value['name'] == prop_name:
                remove_columns.append(column)

    image_cf.remove(image_cf, remove_columns)


def image_member_create(context, values, session=None):
    """Create an ImageMember object"""
    memb = create(Models.ImageMember)
    memb = _image_member_update(context, memb, values)
    return memb


def image_member_update(context, memb_id, values):
    """Update an ImageMember object"""

    memb = _image_member_get(context, memb_id)
    return _image_member_update(context, memb, values, session)


def _image_member_update(context, memb, values):
    """Apply supplied dictionary of values to a Member object."""
    drop_protected_attrs(values)
    values["deleted"] = False
    values.setdefault('can_share', False)

    remove_inverted_indices(memb, Models.ImageMember)

    memb = merge_dict(memb, values)

    save_inverted_indices(memb, Models.ImageMember)

    image_cf.insert(memb['image_id'], {
        memb['id']: dumps(memb)
    })

    return memb


def image_member_delete(context, memb_id, session=None):
    """Delete an ImageMember object"""
    member = _image_member_get(context, memb_id)
    remove_inverted_indices(memb)
    image_cf.remove(memb['image_id'], [memb['id']])


def _image_member_get(context, memb_id):
    """Fetch an ImageMember entity by id"""

    image_ids = query_inverted_indices(Models.ImageMember, 'id', memb_id)

    # TODO: is this reasonable?  Can a member belong to multiple images?
    image_id = image_ids[0]

    image = image_cf.get(image_id)
    column_key = memb_id

    return loads(image[column_key])


def image_member_find(context, image_id=None, member=None, status=None):
    """Find all members that meet the given criteria

    :param image_id: identifier of image entity
    :param member: tenant to which membership has been granted
    """
    members = _image_member_find(context, image_id, member, status)
    return [_image_member_format(m) for m in members]


def _image_member_find(context, image_id=None,
                       member=None, status=None):

    if image.owner == context.owner:
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

    def find_matching_members(image):
        members = []
        for column, value in image.iteritems():
            if column.startswith(MEMBER_PREFIX):
                m = loads(value)
                if criteria.match(m):
                    members.append(m)

        return members

    if image_id:
        image = image_cf.get(image_id)
    
        return find_matching_members(image)

    else:
        image_ids = set()
        if member is not None:
            image_ids = image_ids.union(
                query_inverted_indices(Models.ImageMember, 'member', member))

        if status is not None:
            image_ids = image_ids.union(
                query_inverted_indices(Models.ImageMember, 'status', status))

        members = []
        for image_id in image_ids:
            image = image_cf.get(image_id)
            members.extend(find_matching_members(image))

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
    session = _get_session()
    existing_tags = set(image_tag_get_all(context, image_id, session))
    tags = set(tags)

    tags_to_create = tags - existing_tags
    #NOTE(bcwaldon): we call 'reversed' here to ensure the ImageTag.id fields
    # will be populated in the order required to reflect the correct ordering
    # on a subsequent call to image_tag_get_all
    for tag in reversed(list(tags_to_create)):
        image_tag_create(context, image_id, tag, session)

    tags_to_delete = existing_tags - tags
    for tag in tags_to_delete:
        image_tag_delete(context, image_id, tag, session)


def image_tag_create(context, image_id, value, session=None):
    """Create an image tag."""
    repo.reset()
    tag = repo.create(Models.ImageTag, image_id=image_id, value=value)
    repo.save(tag, Models.ImageTag)
    return tag['value']


def image_tag_delete(context, image_id, value, session=None):
    """Delete an image tag."""
    repo.reset()

    image = repo.load('tags').get(key=image_id)
    tags = filter(lambda x: x['value'] == value and
                            x['deleted'] == False, image['tags'])

    if len(tags) == 0:
        raise exception.NotFound()
    else:
        repo.soft_delete(tags[0])

def image_tag_get_all(context, image_id):
    """Get a list of tags for a specific image."""
    repo.reset()

    image = repo.load('tags').get(key=image_id)
    tags = filter(lambda x: x['deleted'] == False, image['tags'])

    tags = sort_dicts(tags, [('created_at', 'asc')])

    return [tag['value'] for tag in tags]