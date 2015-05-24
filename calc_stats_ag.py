import sys
import math

from pymongo import MongoClient



def calcStats(collection):
	print "Calculating stats for {}:".format(collection.full_name)

	count = collection.count()
	print "Count: {}".format(count)

	median = 0
	cursor = collection.find().sort( {'review/score': 1} )
	if(count%2 == 1):
		median = cursor.skip(count/2-1).limit(1)[0]["review/score"]
	else:
		low = cursor.skip(count/2-1).limit(1)[0]["review/score"]
		high = cursor.skip(count/2).limit(1)[0]["review/score"]
		median = (low+high)/2.0
	print "Median: {}".format(median)

	mode = max(collection.aggregate([\
		{'$match':{'review/score' : {'$exists': True, '$ne' : None}}},\
		{'$group':{'_id': '$review/score', 'count': {'$sum': 1}} }\
		]), key = lambda x:x["count"])["_id"]
	print "Mode: {}".format(mode)

	averages = list(collection.aggregate([\
		{'$match':{'review/score' : {'$exists': True, '$ne' : None}}},\
		{'$group': {\
			'_id': None,\
		 	'mean' : {'$avg' : '$review/score'},\
		 	'meanx2': {'$avg': {'$multiply': ['$review/score', '$review/score']}}}\
		}]))[0]

	avg = averages["mean"]
	avgx2 = averages["meanx2"]
	print "Average: {}".format(avg)

	sd = math.sqrt(avgx2 - math.pow(avg, 2))
	print "Standard Deviation: {}".format(sd)



if __name__ == '__main__':
	client = MongoClient()
	db = client.cs594

	
	calcStats(db.movies)
	calcStats(db.music)
	calcStats(db.games)
	calcStats(db.books)

	
