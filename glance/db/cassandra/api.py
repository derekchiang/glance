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

from oslo.config import cfg

from glance.common import exception

import glance.openstack.common.log as os_logging
from glance.openstack.common import timeutils

from pycassa.pool import ConnectionPool
from pycassa import NotFoundException
from pycassa.system_manager import SystemManager, SIMPLE_STRATEGY

from glance.db.cassandra.models import register_models, unregister_models
from glance.db.cassandra.lib import ImageRepo, \
                                    ImageIdDuplicateException, \
                                    ImageIdNotFoundException, \
                                    Models, \
                                    merge_dict, drop_protected_attrs,\
                                    sort_dicts
from glance.db.cassandra.spec import EQ, Attr, Any, And, Or, NEQ

pool = None
LOG = os_logging.getLogger(__name__)
KEYSPACE_NAME = 'GLANCE'
repo = None

STATUSES = ['active', 'saving', 'queued', 'killed', 'pending_delete',
            'deleted']



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
    global pool, repo

    register_models()

    pool = ConnectionPool(KEYSPACE_NAME,\
                          [CONF.glance_cassandra_url])
    repo = ImageRepo(pool)


def clear_db_env():
    """
    Unset global configuration variables for database.
    """
    global pool
    pool = None


def _check_mutate_authorization(context, image):
    if not is_image_mutable(context, image):
        LOG.info(_("Attempted to modify image user did not own."))
        msg = _("You do not own this image")
        if image['is_public']:
            exc_class = exception.ForbiddenPublicImage
        else:
            exc_class = exception.Forbidden

        raise exc_class(msg)
        

@trace
def image_create(context, values):
    """Create an image from the values dictionary."""
    print 'the values are: '
    print values
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
    repo.reset()

    image = _image_get(context, image_id)

    # Perform authorization check
    _check_mutate_authorization(context, image)

    _image_locations_set(image['id'], [])

    repo.soft_delete(image)

    for prop in image.properties:
        image_property_delete(context, prop.name,
                              image_id)

    members = _image_member_find(context, image_id=image_id)
    for memb in members:
        _image_member_delete(context, memb)

    tag_values = image_tag_get_all(context, image_id)
    for tag_value in tag_values:
        image_tag_delete(context, image_id, tag_value)

    return _normalize_locations(image)

@trace
def _normalize_locations(image):
    print "imageeee:"
    print image
    undeleted_locations = filter(lambda x: not x['deleted'], image['locations'])
    image['locations'] = [{'url': loc['value'],
                           'metadata': loc['meta_data']}
                          for loc in undeleted_locations]
    return image

@trace
def image_get(context, image_id, session=None, force_show_deleted=False):
    image = _image_get(context, image_id,
                       force_show_deleted=force_show_deleted)
    image = _normalize_locations(image)
    print ('finally')
    print (type(image))
    print (image)
    return image

@trace
def _image_get(context, image_id, force_show_deleted=False):
    """Get an image or raise if it does not exist."""
    repo.reset()

    try:
        # TODO: do we want to load everything by default?
        image = repo.load('locations').load('properties')\
                    .load('tags').load('members').get(key=image_id)
        if not force_show_deleted and not _can_show_deleted(context):
            if image.deleted == True:
                raise NotFoundException()
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
    repo.reset()
    filters = filters or {}

    print 'the filters are:'
    print filters

    image_filters = []
    other_filters = []

    if (not context.is_admin) or admin_as_user == True:
        image_filters.append(('is_public', '=', True))
        other_filters.append(Attr('members.deleted', EQ(False)))

        if context.owner is not None:
            image_filters.append(('owner', '=', context.owner))
            other_filters.append(Attr('members.member',
                                      EQ(context, context.owner)))
            if member_status != 'all':
                other_filters.append(Attr('members.status',
                                          EQ(member_status)))

        other_filters = [And(*other_filters)]

    repo.filter(*image_filters)

    if 'visibility' in filters:
        visibility = filters.pop('visibility')
        if visibility == 'public':
            repo.filter(is_public=True)
        elif visibility == 'private':
            repo.filter(is_public=False)
            if context.owner is not None and ((not context.is_admin)
                                              or admin_as_user == True):
                repo.filter(owner=context.owner)
        else:
            other_filters.extend([Attr('members.member', EQ(context.owner)),
                                  Attr('members.deleted', EQ(False))])
            repo.reset()

    if is_public is not None:
        repo.filter(is_public=is_public)

    if 'is_public' in filters:
        # TODO: the spec library does not support 'Any' yet
        other_filters.append(Attr('properties',
                                  Any(And(Attr('name', EQ('is_public')),
                                          Attr('value',
                                               EQ(filters.pop('is_public'))),
                                          Attr('deleted', EQ(False))))))

    showing_deleted = False

    if 'checksum' in filters:
        checksum = filters.get('checksum')
        repo.filter(checksum=checksum)

    if 'changes-since' in filters:
        # normalize timestamp to UTC, as sqlalchemy doesn't appear to
        # respect timezone offsets
        changes_since = timeutils.normalize_time(filters.pop('changes-since'))
        repo.filter(('updated_at', '>', changes_since))
        showing_deleted = True

    if 'deleted' in filters:
        deleted_filter = filters.pop('deleted')
        repo.filter(deleted=deleted_filter)
        showing_deleted = deleted_filter
        # TODO(bcwaldon): handle this logic in registry server
        if not deleted_filter:
            other_filters.append(Attr('status', NEQ('killed')))


    for (k, v) in filters.pop('properties', {}).items():
        other_filters.append(Attr('properties',
                                  Any(And(Attr('name', EQ(k)),
                                          Attr('value',
                                               EQ(v)),
                                          Attr('deleted', EQ(False))))))

    for (k, v) in filters.items():
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
                repo.filter((key, '>=', v))
            elif k.endswith('_max'):
                repo.filter((key, '<=', v))
            elif hasattr(models.Image, key):
                repo.filter((key, '=', v))
            else:
                other_filters.append(Attr('properties',
                                          Any(And(Attr('name', EQ(key)),
                                                  Attr('value',
                                                       EQ(v))))))

    # TODO: Should we load everything by default?
    repo.load('locations').load('properties').load('tags').load('members')
    
    images = repo.get_all()
    other_filters = And(*other_filters)
    images = filter(lambda x: other_filters.match(x), images)

    # TODO: pagination

    return [_normalize_locations(image) for image in repo.get()]

@trace
def _validate_image(values):
    """
    Validates the incoming data and raises a Invalid exception
    if anything is out of order.

    :param values: Mapping of image metadata to check
    """
    LOG.info('values: ')
    LOG.info(values)
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
        image = repo.create(Models.Image)

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

    print 'the image is: '
    print image

    try:
        # TODO: discuss whether we actually want to override
        # stuff here
        repo.save(image, override=True)
    except ImageIdDuplicateException:
        raise exception.Duplicate("Image ID %s already exists!"
                                  % values['id'])

    _set_properties_for_image(context, image, properties, purge_props)

    if location_data is not None:
        _image_locations_set(image['id'], location_data)

    return image_get(context, image['id'])

@trace
def _image_locations_set(image_id, locations):
    repo.reset()

    image = repo.load('locations').get(key=image_id)

    for location in image['locations']:
        if location['deleted'] == False:
            repo.soft_delete(location)

    # TODO: As it currently stands, if there are N locations given,
    # then we will issue N queries to Cassandra, each inserting one
    # location.  But in fact, we could totally insert all locations
    # at once.  We might want to optimize this.
    for location in locations:
        new_location = repo.create(Models.ImageLocation,
                                   image_id=image_id,
                                   value=location['url'],
                                   meta_data=location['metadata'])
        repo.save(new_location, Models.ImageLocation)


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
    prop = repo.create(Models.ImageProperty)
    prop = _image_property_update(context, prop, values)
    return prop


def _image_property_update(context, prop, values):
    """
    Used internally by image_property_create and image_property_update
    """
    repo.reset()
    drop_protected_attrs(values)
    values["deleted"] = False
    prop = merge_dict(prop, values)
    repo.save(prop, Models.ImageProperty)
    return prop


def image_property_delete(context, prop_name, image_id, session=None):
    """
    Used internally by image_property_create and image_property_update
    """
    repo.reset()
    image = repo.load('properties').get(key=image_id)
    properties = image['properties']

    for prop in properties:
        if prop['name'] == prop_name:
            repo.delete(prop)

def image_member_create(context, values, session=None):
    """Create an ImageMember object"""
    memb_ref = models.ImageMember()
    _image_member_update(context, memb_ref, values, session=session)
    return _image_member_format(memb_ref)


def _image_member_format(member_ref):
    """Format a member ref for consumption outside of this module"""
    return {
        'id': member_ref['id'],
        'image_id': member_ref['image_id'],
        'member': member_ref['member'],
        'can_share': member_ref['can_share'],
        'status': member_ref['status'],
        'created_at': member_ref['created_at'],
        'updated_at': member_ref['updated_at']
    }


def image_member_update(context, memb_id, values):
    """Update an ImageMember object"""
    session = _get_session()
    memb_ref = _image_member_get(context, memb_id, session)
    _image_member_update(context, memb_ref, values, session)
    return _image_member_format(memb_ref)


def _image_member_update(context, memb_ref, values, session=None):
    """Apply supplied dictionary of values to a Member object."""
    _drop_protected_attrs(models.ImageMember, values)
    values["deleted"] = False
    values.setdefault('can_share', False)
    memb_ref.update(values)
    memb_ref.save(session=session)
    return memb_ref


def image_member_delete(context, memb_id, session=None):
    """Delete an ImageMember object"""
    session = session or _get_session()
    member_ref = _image_member_get(context, memb_id, session)
    _image_member_delete(context, member_ref, session)


def _image_member_delete(context, memb_ref, session):
    memb_ref.delete(session=session)


def _image_member_get(context, memb_id, session):
    """Fetch an ImageMember entity by id"""
    query = session.query(models.ImageMember)
    query = query.filter_by(id=memb_id)
    return query.one()


def image_member_find(context, image_id=None, member=None, status=None):
    """Find all members that meet the given criteria

    :param image_id: identifier of image entity
    :param member: tenant to which membership has been granted
    """
    session = _get_session()
    members = _image_member_find(context, session, image_id, member, status)
    return [_image_member_format(m) for m in members]


def _image_member_find(context, session, image_id=None,
                       member=None, status=None):
    query = session.query(models.ImageMember)
    query = query.filter_by(deleted=False)

    if not context.is_admin:
        query = query.join(models.Image)
        filters = [
            models.Image.owner == context.owner,
            models.ImageMember.member == context.owner,
        ]
        query = query.filter(sa_sql.or_(*filters))

    if image_id is not None:
        query = query.filter(models.ImageMember.image_id == image_id)
    if member is not None:
        query = query.filter(models.ImageMember.member == member)
    if status is not None:
        query = query.filter(models.ImageMember.status == status)

    return query.all()


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