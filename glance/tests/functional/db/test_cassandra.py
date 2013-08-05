# Copyright 2012 OpenStack Foundation
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


import glance.db.cassandra.api
from glance.db.cassandra import models as db_models
import glance.tests.functional.db as db_tests
from glance.tests.functional.db import base


def get_db(config):
    # config(sql_connection='sqlite://', verbose=False, debug=False)
    db_api = glance.db.cassandra.api
    db_api.setup_db_env()
    return db_api


def reset_db(db_api):
    db_models.unregister_models()
    db_models.register_models()


class TestCassandraDriver(base.TestDriver, base.DriverTests):

    def setUp(self):
        db_tests.load(get_db, reset_db)
        super(TestCassandraDriver, self).setUp()
        self.addCleanup(db_tests.reset)

    # Override tests that are not relevant to the Cassandra driver
    def test_image_destroy(self):
        pass


class TestCassandraVisibility(base.TestVisibility, base.VisibilityTests):

    def setUp(self):
        db_tests.load(get_db, reset_db)
        super(TestCassandraVisibility, self).setUp()
        self.addCleanup(db_tests.reset)


class TestCassandraMembershipVisibility(base.TestMembershipVisibility,
                                         base.MembershipVisibilityTests):

    def setUp(self):
        db_tests.load(get_db, reset_db)
        super(TestCassandraMembershipVisibility, self).setUp()
        self.addCleanup(db_tests.reset)
