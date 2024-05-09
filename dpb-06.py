import pymongo
import pprint

client = pymongo.MongoClient()
collection = client.cv06.restaurants

# pprint.pprint(list(collection.find()))

# # 1. (a)
# pprint.pprint(list(collection.aggregate([
#     { "$group": { "_id": "$address.zipcode", "restaurants": { "$push": "$$ROOT" } } },
#     { "$sort": { "_id": 1 } },
#     { "$group": { "_id": None, "count": { "$sum": 1 } } },
#     { "$project": { "psc": "$address.zipcode", "count": "$count" } },
#     { "$limit": 10 }
# ])))

# 1. (b)
# pprint.pprint(list(collection.aggregate([
#     { "$group": { "_id": "$address.zipcode", "restaurants": { "$push": "$$ROOT" } } },
#     { "$addFields": { "count": { "$size": "$restaurants" } } },
#     { "$sort": { "count": -1 } },
#     { "$project": { "psc": "$address.zipcode", "count": "$count" } },
#     { "$limit": 10 }
# ])))

# 2. 
# pprint.pprint(list(collection.aggregate([
#     { "$addFields": { "grades": { "$filter": { "input": "$grades", "as": "grade", "cond": { "$ne": ["$$grade.grade", "Not Yet Graded"] } } } } },
#     { "$unwind": "$grades" },
#     { "$group": { "_id": "$grades.grade", "score_avg": { "$avg": "$grades.score" } } },
#     # { "$group": { "_id": { "rest_id": "$restaurant_id", "grade": "$grades.grade" }, "grade_avg": { "$avg": "$grades.score" } } },
#     { "$limit": 10 }
# ])))

# Bonus 1.
pprint.pprint(list(collection.aggregate([
    { "$addFields": { "grades": { "$filter": { "input": "$grades", "as": "grade", "cond": { "$ne": ["$$grade.grade", "Not Yet Graded"] } } } } },
    { "$unwind": "$grades" },
    { "$group": { 
        "_id": { "name": "$name", "grade": "$grades.grade" }, 
        "grade_avg": { "$avg": "$grades.score" }, 
        "grade_count": { "$sum": 1 }
    } },
    { "$match": { "grade_count": { "$gt": 2 } } },
    { "$match": { "_id.grade": "A" } },
    { "$sort": { "grade_avg": -1 } },
    { "$limit": 5 }
])))

# Bonus 2.
# pprint.pprint(list(collection.aggregate([
#     { "$addFields": { "grades": { "$filter": { "input": "$grades", "as": "grade", "cond": { "$ne": ["$$grade.grade", "Not Yet Graded"] } } } } },
#     { "$unwind": "$grades" },
#     { "$group": { 
#         "_id": { "name": "$name", "grade": "$grades.grade", "cuisine": "$cuisine" }, 
#         "grade_avg": { "$avg": "$grades.score" }, 
#         "grade_count": { "$sum": 1 }
#     } },
#     { "$match": { "grade_count": { "$gt": 2 } } },
#     { "$match": { "_id.grade": "A" } },
#     { "$group": { "_id": "$_id.cuisine", "restaurants": { "$top": { "sortBy": { "grade_avg": -1 }, "output": [ "$_id.name", "$grade_avg" ] } } } }
# ])))

# Bonus 3.
# pprint.pprint(list(collection.aggregate([
#     { "$match": { "$expr": { "$gt": [{ "$size": { "$split": ["$name", " "] } }, 1] } } },
#     { "$addFields": { "grades": { "$filter": { "input": "$grades", "as": "grade", "cond": { "$ne": ["$$grade.grade", "Not Yet Graded"] } } } } },
#     { "$match": { "$expr": {
#         "$gt": [{ "$size": { "$filter": { "input": "$grades", "as": "grade", "cond": { "$gt": ["$$grade.score", 10] } } } }, 1]
#     } } }
# ])))
