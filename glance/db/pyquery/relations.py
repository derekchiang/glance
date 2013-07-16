class Relation(object):
    def __init__(self, modelA, ref_to_b, modelB, ref_to_a):
        self.modelA = modelA
        self.modelB = modelB
        self.ref_to_a = ref_to_a
        self.ref_to_b = ref_to_b

    def first(self, model, specs):
        self.fetch(model, specs, 1)

    def fetch(self, model, specs, number):
        self.all(model, specs)[:number]

    def all(self, model, specs):
        pass

    def delete(self, model, specs):
        pass

    def insert(self, model, specs, values):
        pass

class OneToOne(Relation):
    pass

class OneToMany(Relation):
    pass

class ManyToOne(Relation):
    pass

class ManyToMany(Relation):
    pass

def add_relation(relation):
    relation.modelA.add_relation(relation)
    relation.modelB.add_relation(relation)

class RelationRepo(object):
    def __init__(self):
        self.relations = []

    def add_relation(self, relation):
        self.relations.append(relation)

    def get_related_model(self, model, ref_name):
        for relation in self.relations:
            if relation.modelA == model and relation.ref_to_b == ref_name:
                return relation.modelB
            elif relation.modelB == model and relation.ref_to_a == ref_name:
                return relation.modelA
        return None