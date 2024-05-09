import pymongo
from bson.objectid import ObjectId

import pprint
import datetime

client = pymongo.MongoClient()
collection = client.cv04.restaurants

# # 1.
# inserted = collection.insert_many([
#     { "name": "U Balcaru", "borough": "Liberec", "cuisine": "Czech", "grades": [] },
#     { "name": "Samargatha", "borough": "Liberec", "cuisine": "Indian", "grades": [] },
#     { "name": "Sapa", "borough": "Liberec", "cuisine": "Thai", "grades": [] }
# ])

# # 2.
# pprint.pprint(list(collection.find({ "_id": { "$in": inserted.inserted_ids }})))

# # 3.
# collection.update_one({ "name": { "$eq": "U Balcaru" } }, { "$set": { "name": "Novy U Balcaru" }})

# # 4.
# collection.update_one(
#     { "name": { "$eq": "Novy U Balcaru" }}, 
#     { "$push": { "grades": { "date": datetime.datetime.now(), "grade": "C", "score": 70 } } }
# )

# # 5.
# collection.delete_many({ "_id": { "$in": inserted.inserted_ids } })


# 1.
# pprint.pprint(list(collection.find()))

# # 2.
# cursor = collection.find().sort({ "name": pymongo.ASCENDING })
# pprint.pprint(list(collection.find().sort({ "name": pymongo.ASCENDING })))

# # 3.
# pprint.pprint(list(cursor.rewind().limit(10)))

# # 4.
# pprint.pprint(list(cursor.rewind().skip(10).limit(10)))

# # 5.
# pprint.pprint(list(collection.find({ "borough": { "$eq": "Bronx" } })))

# 6.
# pprint.pprint(list(collection.find({ "$where": """function() { return this.name.startsWith("M"); }""" })))

# 7.
# pprint.pprint(list(collection.find({ "borough": { "$eq": "Manhattan" }, "cuisine": { "$eq": "Italian" } })))

# 8.
# pprint.pprint(list(collection.find({ "grades": { "$elemMatch": { "score": { "$gt": 80 } } } })))


# # Bonus 1.
# pprint.pprint(list(collection.find({ "grades": { "$elemMatch": { "score": { "$lt": 90, "$gt": 80 } } } })))

# Bonus 2.
collection.update_many(
    { "grades": { "$elemMatch": { "score": { "$gt": 80 } } } }, 
    { "$set": { "grades.$.popular": 1 } }
)

# # Bonus 3.
# collection.update_many(
#     { "grades": { "$elemMatch": { "score": { "$lt": 1 } } } }, 
#     { "$set": { "grades.$.trash": 1 } }
# )

# # Bonus 4.
# pprint.pprint(list(collection.find({ 
#     "popular": { "$eq": 1 }, 
#     "trash": { "$eq": 1 } 
# })))

# # Bonus 5.
collection.update_many(
    { "grades": { "$elemMatch": { "score": { "$gt": 90 } } } }, 
    { "$set": { "grades.$.top_score": 1 } }
)