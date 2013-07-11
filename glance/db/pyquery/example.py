from model import Model
from relations import OneToMany, add_relation
import sqlalchemy
from sqlalchemy import Column, Integer, ForeignKey, relationship, backref

from pycassa.types import IntegerType, BytesType, deserialize
import pycassa

from query import Query
from spec import EqualTo

# An actual Nova API, implemented using the generic framework
def compute_node_get(context, compute_id):
    result = Query(ComputeNode).filter(EqualTo('id', compute_id)).first()
    return result

# Generic declaration

class Model(object):
    is_standalone = True
    relations = []

class Service(Model):
    pass

class ComputeNode(Model):
    pass

class ServiceHasManyComputeNode(OneToMany):
    def query(attributes):
        pass

# This method adds the relationship to both class' `relations` field
add_relation(ServiceHasManyComputeNode(Service, ComputeNode))

# Generic definition of Query
class GenericQuery(object):
    def __init__(self, model):
        self.model = model
        # some other stuff...

    # some other methods...

    def first(self):
        if self.model.is_standalone:
            self.get(self.attribute, 1)
        else:
            for rel in self.model.relations:
                result = rel.query(self.attribute)
                if result is not None:
                    return result

    # this method should be implemented by the driver
    def get(attribute, number):
        pass

# SQLAlchemy

class Service(Model, sqlalchemy.Model):
    id = Column(Integer, primary_key=True)

class ComputeNode(Model, sqlalchemy.Model):
    id = Column(Integer, primary_key=True)
    service_id = Column(Integer, ForeignKey('services.id'), nullable=False)
    service = relationship(Service,
                           backref=backref('compute_node'),
                           foreign_keys=service_id,
                           primaryjoin='and_('
                                'ComputeNode.service_id == Service.id,'
                                'ComputeNode.deleted == 0)')

class Query(GenericQuery):
    def get(attribute, number):
        if isinstance(attribute, EqualTo):
            # I know this is not valid python code, but you get the idea
            results = sqlalchemy.query(self.model).filter(EqualTo.attr=EqualTo.value).all()
            return results[:number]

# Cassandra

class Service(Model):
    id = IntegerType()
    # Suppose Service stores ComputeNodes as serialized JSON data
    compute_nodes = BytesType()

class ComputeNode(Model):
    is_standalone = False

class ServiceHasManyComputeNode(OneToMany):
    def query(attribute):
        data = pycassa.query(Service).get_all('compute_nodes')
        compute_nodes = deserialize(data)
        for cn in compute_nodes:
            if attribute.match(cn):
                yield cn

        return None

add_relation(ServiceHasManyComputeNode(Service, ComputeNode))

class Model(object):
    is_standalone = True
    relations = []

    def __init__(self, object):
        pass

    def save(self, session=None):
        pass

    def delete(self, session=None):
        pass

    @classmethod
    def add_relation(cls, relation):
        cls.relations.push(relation)




class HasMany(object):
    def __init__(self, parent, children, parentName, childrenName):
        self.parent = parent
        self.children = children
        self.parentName = parentName
        self.childrenName = childrenName

        # parent

class ServiceHasManyComputeNode(object):
    def queryChildren(self):
        pass

    def queryParent(self):
        pass

    def queryJoin(self, spec):
        query(Service).join(ComputeNode).filter(spec)


# Sample queries:

addRelation(ServiceHasManyComputeNode('compute_nodes', 'service'))

query(models.Service).join_filter('compute_nodes', EQ('name', 'NOVA'))

if compute_nodes is not standalone:
    relation = get_relation

    relation -> join_filter EQ

    deserialize compute_nodes lawdwoi

def join_filter(join_name, spec):
    relation = get_relation(models.Service, 'compute_nodes')
    relation.get('compute_nodes')

query(models.ComputeNode).join_filter('service', EQ('id', 123))

conclusion:

1. call join_filter on query object
2. when first() is invoked, check to see if model is standalone, and then call first() accordingly
3. if model is standalone, inside query implementation, check to see if the joined table is standalone
4. if the joined model is standalone, then use SQLAlchemy to join directly
5. if it is not standalone, then figure out the relationship, and call join_filter on that
6. if the model is not standalone, then figure out the relationship and call first()
7. Inside the relationship implementation, figure out if the table being joined is standalone;
8. If it is standalone, 

Joining two standalone tables:

    rely on the query implementation

    SQLAlchemy: join(model).query(specs)
    Cassandra: rely on relations

querying a standalone table joined with a non-standalone table

    rely on relations

    SQLAlchemy: 