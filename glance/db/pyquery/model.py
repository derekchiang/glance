class Model(object):
    is_standalone = True
    relations = []

    def __init__(self, object):
        pass

    def save(self, session=None):
        pass

    def update(self, values):
        pass

    def delete(self, session=None):
        pass

    @classmethod
    def add_relation(cls, relation):
        cls.relations.push(relation)