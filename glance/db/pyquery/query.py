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

from spec import Attr, EQ

def get_session():
    return Session()

class Session(object):
    def begin(self):
        return DummySession()

class DummySession(object):
    def __enter__(self):
        return None

    def __exit__(self):
        pass

class QueryImplementation(object):

    """
    Drivers need to implement a class with this interface.

    Unless specified otherwise, the arguments to the following methods
    have the following structure:

    specs: a dictionary, like: { age: 13, name: "Derek" }

    joins: a list of tuples of a model and a key, like: [ (models.Children,
    'name'), (models.Friend, 'age') ]

    orders: a list of dictionary, like: { age: 'asc', name: 'decs' }
    """

    def first(self, model, specs, joins, orders, session):
        self.fetch(specs, 1)

    def fetch(self, model, specs, joins, orders, number, session):
        self.all(specs)[:number]

    def all(self, model, specs, joins, orders, session):
        pass

    def delete(self, model, specs, session):
        pass

    def insert(self, model, specs, values, session):
        pass

    def save(self, session):
        pass


class Query(object):

    """
    An abstraction for efficiently querying images from a repository.
    """

    def __init__(self, model=None, session=None, query_impl=None):
        self.specs = []
        self.orders = {}
        self.joins = []
        self.join_filters = []
        self.model = model
        self.session = session
        self.impl = query_impl

    def __call__(self, model, session=None):
        return Query(model, session, self.impl)

    def call_impl(func):
        def wrapper(self, *args):
            if self.model.is_standalone:
                return getattr(self.impl, func.__name__)(*args, **self.__dict__)
            else:
                for rel in self.model.relations:
                    result = getattr(rel, func.__name__)(*args, **self.__dict__)
                    if result is not None:
                        return result

                return None
        return wrapper

    def filter(self, *args, **kwargs):
        if len(args) == 0:
            for k, v in kwargs.iteritems():
                self.specs.append(Attr(k, EQ(v)))
        else:
            self.specs.extend(args)
        return self

    filter_by = filter

    @call_impl
    def exist(self):
        """
        Return true if any record that matches the given filters exist,
        or false otherwise
        """
        pass

    @call_impl
    def first(self):
        """
        Return all images that match this query.

        :returns a list of images, where each image takes the form of a dict
        """
        pass

    @call_impl
    def fetch(self, number):
        pass

    @call_impl
    def all(self):
        pass

    @call_impl
    def delete(self):
        """
        Remove all images that match this query from the underlying repository

        :returns how many images were deleted
        """
        pass

    @call_impl
    def insert(self, values):
        """
        Update all images that match this query with the given values.

        :param image_update: a dictionary of updates
        :returns how many images were updated
        """
        pass

    @call_impl
    def join_filter(self, join_name, spec):
        """
        1. get the field given by join_name
        2. search the field using the given spec
        3. if any record in the field matches the given spec, return True.
        Else return False.
        """
        self.join_filters.append((join_name, spec))
        return self

    def joinload(self, key, model=None):
        if model is None:
            model = self.model
        self.joins.append((model, key))
        return self

    def order_by(self, key, order="asc"):
        self.orders[key] = order
        return self