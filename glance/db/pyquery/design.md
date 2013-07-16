# PyQuery Design Documentation

PyQuery aims to bridge the gap between NoSQL and SQL.  The end goal is to allow you to write your database code using PyQuery APIs, and the code will just work with SQL databases *and* NoSQL databases.

As with any framework, an attempt at generalization will inevitably result in inefficiency in some cases.  Therefore, PyQuery allows you to dive into lower levels and use specific drivers of your choice directly.

## Overview

There are five entities in PyQuery that you should be familiar with:

1. Model
2. Relation
3. Spec
4. SpecTranslator
5. Query
6. Driver

In the following documentation, we are going to assume that you want to build a student management application.  The application groups students into schools.  Each school and student has their own metadata.  Just for the sake of discussion, let's say a student can also have many cats.

### Model

A model represents, well, a model.  PyQuery's models do not contain any concrete data types.  It's a driver's job to actually associate concrete datatypes with a model.

You, as a user of PyQuery, are expected to declare models that your applications will use.  For example:

	class School(Model):
		students = HasMany(Student)

	class Student(Model):
		school = HasOne(School)
		pets = HasMany(Pet)

	class Pet(Model):
		owner = HasOne(Student)

Note that it's not important to declare all fields at this stage.  You will do that when you implement a specific DB driver.  What's necessary is to state the relationship between models.  In this case, we have a has-many relationship between schools and students.

### Relation

Relations are what define an RDBMS, and what NoSQL databases remarkably lack.  It's on relations where PyQuery actually shine.

When we declare that a school has many students, ultimately what we are trying to do is to be able to answer the following questions:

1. How do we get the students associated with a certain school?
2. How do we get the students in a school with the name "Nova"?
3. How do we get the students in a school named "Nova" who have the name "Derek"?
4. How do we get the schools with a student named "Derek"?
5. How do we get the schools with the name "Nova" and a student named "Derek"?

All these questions will need to be answered by relations.

The key observation here is that, while we have many logical models, some of them do not necessarily have underlying database tables, depending on the actual DB backend.  A Cassandra backend might have a huge table of schools, where students are serialized into bytearrays and stored in the table, along with their pets.

The way PyQuery handles this is through relations.  When you ask for a student with the name "Derek", PyQuery checks to see if a "student" table actually exist.  If it does, then PyQuery goes ahead and fetch it.  If not, PyQuery looks into what relations the student model has.  In this case, PyQuery will find out that a student has one school.  Then PyQuery will ask the school model: "can you find a student with the name Derek in any school"?  The school model should be programmed to answer this question via a query() method:

Here is a pseudo-implementation of the student-school relationship with Cassandra:

	class Student:
		resides = ['School.students']

		@classmethod
		def query(cls, field, spec, data):
			if field == 'pets':


	class School:
		students = HasMany(Student)

		@classmethod
		def query(cls, field, spec):
			if field == 'students':
				schools = get_all_schools()
				for s in schools:
					students = deserialize(s.students)
					...
			else:
				throw NotQueryable()

	class Pet:
		resides = ['Student.pet']


Note that the query() method needs to be a class method, and that at the end it should throw a NotQueryable exception if the given field is not available for query, in which case PyQuery might use another relationship or throw an exception if no other relationships are available.

The question "How do we get the schools with a student named Derek?", when written in PyQuery, looks like:

	query(School).join_filter('students', EQ('name', 'Derek'))

PyQuery checks if the model that "students" point to, which is Student in this case, actually has an underlying model.  If so (in the case of SQLAlchemy), PyQuery translate the query above to a SQLAlchemy equivalent.  If not (in the case of Cassandra), PyQuery first applies all other filters, and then use the query() method to see if any returned students is of a school, and finally return the final list of schools.

Now, what happens when I when to query a pet, which in Cassandra's case is a serialized model inside another serialized model (student)?

PyQuery first checks to see that Pet is not standalone, then it sees that Student is also not standalone, then it sees that School is.  Now it use School.query() to get all students, and then Student.query() to get all pets, and finally run the filters on those pets.