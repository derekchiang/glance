from glance.db.pyquery.relations import RelationRepo, OneToMany
from glance.db import get_backend

models = get_backend('models')
# print models

# class Image(Model):
#     is_standalone = True

# class ImageProperty(Model):
#     is_standalone = True

# class ImageTag(Model):
#     is_standalone = True

# class ImageLocation(Model):
#     is_standalone = True

# class ImageMember(Model):
#     is_standalone = True

repo = RelationRepo()

def register_relationships(repo):
    repo.add_relation(OneToMany(models.Image, 'locations', models.ImageLocation, 'image'))
    repo.add_relation(OneToMany(models.Image, 'properties', models.ImageProperty, 'image'))
    repo.add_relation(OneToMany(models.Image, 'members', models.ImageMember, 'image'))

register_relationships(repo)

def get_related_model(model, ref_name):
    return repo.get_related_model(model, ref_name)