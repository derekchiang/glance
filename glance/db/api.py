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
from glance.db import get_backend
from glance.common import exception
import glance.openstack.common.log as os_logging
from glance.openstack.common import timeutils
from glance.openstack.common.gettextutils import _
from pyquery.query import Query
from pyquery.spec import Attr, EQ, GT, LT, And, Or, NEQ, inspect_attr

_ENGINE = None
_MAKER = None
_MAX_RETRIES = None
_RETRY_INTERVAL = None
sa_logger = None
LOG = os_logging.getLogger(__name__)


STATUSES = ['active', 'saving', 'queued', 'killed', 'pending_delete',
            'deleted']

# Set up query -- should read from a config file eventually

backend = get_backend()

# TODO: why is this not working?
exceptions = get_backend('exceptions')
models = get_backend('models')

# For backward compatibility
setup_db_env = backend.api.setup_db_env
clear_db_env = backend.api.clear_db_env

def get_session():
    return backend.api._get_session()

get_query = Query(query_impl=backend.query_impl)

def _check_mutate_authorization(context, image_ref):
    if not is_image_mutable(context, image_ref):
        LOG.info(_("Attempted to modify image user did not own."))
        msg = _("You do not own this image")
        if image_ref.is_public:
            exc_class = exception.ForbiddenPublicImage
        else:
            exc_class = exception.Forbidden

        raise exc_class(msg)


def image_create(context, values):
    """Create an image from the values dictionary."""
    return _image_update(context, values, None, False)


def image_update(context, image_id, values, purge_props=False):
    """
    Set the given properties on an image and update it.

    :raises NotFound if image does not exist.
    """
    return _image_update(context, values, image_id, purge_props)


def image_destroy(context, image_id):
    """Destroy the image or raise if it does not exist."""
    session = get_session()
    with session.begin():
        image_ref = _image_get(context, image_id, session=session)

        # Perform authorization check
        _check_mutate_authorization(context, image_ref)

        _image_locations_set(image_ref.id, [], session)

        image_ref.delete(session=session)

        for prop_ref in image_ref.properties:
            image_property_delete(context, prop_ref, session=session)

        members = _image_member_find(context, session, image_id=image_id)
        for memb_ref in members:
            _image_member_delete(context, memb_ref, session)

    return _normalize_locations(image_ref.to_dict())


def _normalize_locations(image):
    undeleted_locations = filter(lambda x: not x.deleted, image['locations'])
    image['locations'] = [loc['value'] for loc in undeleted_locations]
    return image


def image_get(context, image_id, session=None, force_show_deleted=False):
    assert image_id is not None

    image = _image_get(context, image_id, session=session,
                       force_show_deleted=force_show_deleted)
    image = _normalize_locations(image.to_dict())
    return image


def _image_get(context, image_id, session=None, force_show_deleted=False):
    """Get an image or raise if it does not exist."""
    session = session or get_session()
    query = get_query(models.Image, session=session).filter(Attr('id', EQ(image_id))).joinload('properties').joinload('locations')

    # filter out deleted images if context disallows it
    if not force_show_deleted and not _can_show_deleted(context):
        query = query.filter(Attr('deleted', EQ(False)))

    image = query.first()

    if image is None:
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
    assert(not (sort_dir and sort_dirs))
    if sort_dirs is None:
        sort_dirs = [sort_dir for _ in sort_keys]
    assert(len(sort_dirs) == len(sort_keys))

    criterions = []
    for index, key in enumerate(sort_keys):
        query.order_by(key, sort_dirs[index])
        if marker is not None:
            attr = getattr(marker, key)
            filters = []
            for i in range(index):
                filters.append(Attr(key, EQ(attr)))
            if sort_dirs[index] == 'asc':
                filters.append(Attr(key, GT(attr)))
            elif sort_dirs[index] == 'desc':
                filters.append(Attr(key, LT(attr)))
            criterions.append(And(*filters))

    if criterions is not []:
        query.filter(*criterions)

    return query.fetch(limit)


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
    query = get_query(models.Image)

    if (not context.is_admin) or admin_as_user == True:
        visibility_filters = [ Attr('is_public', EQ(True)) ]
        member_filters = [ Attr('members.deleted', EQ(False)) ]
        
        if context.owner is not None:
            if member_status == 'all':
                visibility_filters.extend([ Attr('owner', EQ(context.owner)) ])
                member_filters.extend([ Attr('members.member', EQ(context.owner)) ])
            else:
                visibility_filters.extend([ Attr('owner', EQ(context.owner)) ])
                member_filters.extend([ Attr('members.member', EQ(context.owner)), Attr('members.status', EQ(member_status)) ])

            # sanity check
            assert (len(visibility_filters) == 2)
            assert (2 <= len(member_filters) <= 3)

    query = query.filter(Or(*[Or(*visibility_filters), And(*member_filters)]))

    if 'visibility' in filters:
        visibility = filters.pop('visibility')
        if visibility == 'public':
            query = query.filter(Attr('is_public', EQ(True)))
        elif visibility == 'private':
            query = query.filter(Attr('is_public', EQ(False)))
            if context.owner is not None and ((not context.is_admin)
                                              or admin_as_user == True):
                query = query.filter(owner = context.owner)
        else:
            query.join_filter('members', And(Attr('member', EQ(context.owner)), Attr('deleted', EQ(False))))

    if is_public is not None:
        query = query.filter(Attr('is_public', EQ(is_public)))

    if 'is_public' in filters:
        # This will match all properties that satisfy any of the critirion
        # but you want the properties that match all of them
        query.filter(And(Attr('name', EQ('is_public')),
                         Attr('value', EQ(filters.pop('is_public'))),
                         Attr('deleted', EQ(False))))

    showing_deleted = False

    if 'changes-since' in filters:
        changes_since =timeutils.normalize_time(filters.pop('changes-since'))
        query = query.filter(Attr('updated_at', GT(changes_since)))
        showing_deleted = True

    if 'deleted' in filters:
        deleted_filter = filters.pop('deleted')
        query = query.filter(deleted=deleted_filter)
        showing_deleted = deleted_filter
        if not deleted_filter:
            query.filter(Attr('status', NEQ('killed')))


    for (k, v) in filters.pop('properties', {}).items():
        query = query.join_filter('properties', And(Attr('name', EQ(k)),
                                                    Attr('value', EQ(v)),
                                                    Attr('deleted', EQ(False))))

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
                query = query.filter(Attr(getattr(models.Image, key), GT(v)))
            elif k.endswith('_max'):
                query = query.filter(Attr(getattr(models.Image, key), LT(v)))
            elif hasattr(models.Image, key):
                query = query.filter(Attr(getattr(models.Image, key), EQ(v)))
            else:
                query = query.join_filter('properties',
                    And(Attr('name', EQ(k)),
                        Attr('value', EQ(v))))

    marker_image = None
    if marker is not None:
        marker_image = _image_get(context, marker,
                                  force_show_deleted=showing_deleted)

    query = query.joinload('properties').joinload('locations')

    images = _paginate_query(query, models.Image, limit,
                            [sort_key, 'created_at', 'id'],
                            marker=marker_image,
                            sort_dir=sort_dir)

    return [_normalize_locations(image.to_dict()) for image in images]


def _drop_protected_attrs(model_class, values):
    """
    Removed protected attributes from values dictionary using the models
    __protected_attributes__ field.
    """
    for attr in model_class.__protected_attributes__:
        if attr in values:
            del values[attr]


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


def _image_update(context, values, image_id, purge_props=False):
    """
    Used internally by image_create and image_update

    :param context: Request context
    :param values: A dict of attributes to set
    :param image_id: If None, create the image, otherwise, find and update it
    """
    session = get_session()
    with session.begin():

        # Remove the properties passed in the values mapping. We
        # handle properties separately from base image attributes,
        # and leaving properties in the values mapping will cause
        # a SQLAlchemy model error because SQLAlchemy expects the
        # properties attribute of an Image model to be a list and
        # not a dict.
        properties = values.pop('properties', {})

        try:
            locations = values.pop('locations')
            locations_provided = True
        except KeyError:
            locations_provided = False

        if image_id:
            image_ref = _image_get(context, image_id, session=session)

            # Perform authorization check
            _check_mutate_authorization(context, image_ref)
        else:
            if values.get('size') is not None:
                values['size'] = int(values['size'])

            if 'min_ram' in values:
                values['min_ram'] = int(values['min_ram'] or 0)

            if 'min_disk' in values:
                values['min_disk'] = int(values['min_disk'] or 0)

            values['is_public'] = bool(values.get('is_public', False))
            values['protected'] = bool(values.get('protected', False))
            image_ref = models.Image()

        # Need to canonicalize ownership
        if 'owner' in values and not values['owner']:
            values['owner'] = None

        if image_id:
            # Don't drop created_at if we're passing it in...
            _drop_protected_attrs(models.Image, values)
            #NOTE(iccha-sethi): updated_at must be explicitly set in case
            #                   only ImageProperty table was modifited
            values['updated_at'] = timeutils.utcnow()
        image_ref.update(values)

        # Validate the attributes before we go any further. From my
        # investigation, the @validates decorator does not validate
        # on new records, only on existing records, which is, well,
        # idiotic.
        values = _validate_image(image_ref.to_dict())
        _update_values(image_ref, values)

        try:
            image_ref.save(session=session)
        # TODO: shouldn't 
        except exceptions.DuplicateKeyError:
            raise exception.Duplicate("Image ID %s already exists!"
                                      % values['id'])

        _set_properties_for_image(context, image_ref, properties, purge_props,
                                  session)

    if locations_provided:
        _image_locations_set(image_ref.id, locations, session)

    return image_get(context, image_ref.id)


def _image_locations_set(image_id, locations, session):
    location_refs = get_query(models.ImageLocation, session)\
                           .filter_by(Attr('image_id', EQ(image_id)))\
                       .filter_by(Attr('deleted', EQ(False)))\
                           .all()
    for location_ref in location_refs:
        location_ref.delete(session=session)

    for location in locations:
        location_ref = models.ImageLocation(image_id=image_id, value=location)
        location_ref.save()


def _set_properties_for_image(context, image_ref, properties,
                              purge_props=False, session=None):
    """
    Create or update a set of image_properties for a given image

    :param context: Request context
    :param image_ref: An Image object
    :param properties: A dict of properties to set
    :param session: A SQLAlchemy session to use (if present)
    """
    orig_properties = {}
    for prop_ref in image_ref.properties:
        orig_properties[prop_ref.name] = prop_ref

    for name, value in properties.iteritems():
        prop_values = {'image_id': image_ref.id,
                       'name': name,
                       'value': value}
        if name in orig_properties:
            prop_ref = orig_properties[name]
            _image_property_update(context, prop_ref, prop_values,
                                   session=session)
        else:
            image_property_create(context, prop_values, session=session)

    if purge_props:
        for key in orig_properties.keys():
            if key not in properties:
                prop_ref = orig_properties[key]
                image_property_delete(context, prop_ref, session=session)


def image_property_create(context, values, session=None):
    """Create an ImageProperty object"""
    prop_ref = models.ImageProperty()
    return _image_property_update(context, prop_ref, values, session=session)


def _image_property_update(context, prop_ref, values, session=None):
    """
    Used internally by image_property_create and image_property_update
    """
    _drop_protected_attrs(models.ImageProperty, values)
    values["deleted"] = False
    prop_ref.update(values)
    prop_ref.save(session=session)
    return prop_ref


def image_property_delete(context, prop_ref, session=None):
    """
    Used internally by image_property_create and image_property_update
    """
    prop_ref.delete(session=session)
    return prop_ref


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
    session = get_session()
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
    session = session or get_session()
    member_ref = _image_member_get(context, memb_id, session)
    _image_member_delete(context, member_ref, session)


def _image_member_delete(context, memb_ref, session):
    memb_ref.delete(session=session)


def _image_member_get(context, memb_id, session):
    """Fetch an ImageMember entity by id"""
    query = get_query(models.ImageMember)
    query = query.filter(Attr('id', EQ(memb_id)))
    return query.first()


def image_member_find(context, image_id=None, member=None, status=None):
    """Find all members that meet the given criteria

    :param image_id: identifier of image entity
    :param member: tenant to which membership has been granted
    """
    session = get_session()
    members = _image_member_find(context, session, image_id, member, status)
    return [_image_member_format(m) for m in members]


def _image_member_find(context, session, image_id=None,
                       member=None, status=None):
    query = get_query(models.ImageMember, session=session)
    query = query.filter(Attr('deleted', EQ(False)))

    if not context.is_admin:
        filters = [
            Attr('image.owner', EQ(context.owner)),
            Attr('member', EQ(context.owner)),
        ]
        query = query.filter(Or(*filters))

    if image_id is not None:
        query = query.filter(Attr('image_id', EQ(image_id)))
    if member is not None:
        query = query.filter(Attr('member', EQ(member)))
    if status is not None:
        query = query.filter(Attr('status', EQ(status)))

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
    session = get_session()
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
    session = session or get_session()
    tag_ref = models.ImageTag(image_id=image_id, value=value)
    tag_ref.save(session=session)
    return tag_ref['value']


def image_tag_delete(context, image_id, value, session=None):
    """Delete an image tag."""
    session = session or get_session()
    query = get_query(models.ImageTag)\
                   .filter(image_id=image_id)\
                   .filter(value=value)\
                   .filter(deleted=False)
    try:
        tag_ref = query.first()
    except exceptions.NoResultFoundError:
        raise exception.NotFound()

    tag_ref.delete(session=session)


def image_tag_get_all(context, image_id, session=None):
    """Get a list of tags for a specific image."""
    session = session or get_session()
    tags = get_query(models.ImageTag)\
                  .filter_by(image_id=image_id)\
                  .filter_by(deleted=False)\
                  .order_by('created_at')\
                  .all()
    return [tag['value'] for tag in tags]
