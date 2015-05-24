import sys
import math

from pymongo import MongoClient



def calcStats(collection):
	print "Calculating stats for {}:".format(collection.full_name)

	count = collection.count()
	print "Count: {}".format(count)

	# median = 0
	# cursor = collection.find({}, {"review/score": 1}).sort( "review/score" )
	# if(count%2 == 1):
	# 	median = cursor.hint("review/score_1")[count/2]["review/score"]
	# else:
	# 	low = cursor.hint("review/score_1")[count/2]["review/score"]
	# 	high = cursor.hint("review/score_1")[count/2+1]["review/score"]
	# 	median = (low+high)/2.0
	# print "Median: {}".format(median)

	mode_c = list(collection.aggregate([\
			{'$match':{'review/score' : {'$exists': True, '$ne' : None}}},\
			{'$group':{'_id': '$review/score', 'count': {'$sum': 1}} }\
			]))
	mode = max(mode_c, key = lambda x:x["count"])["_id"]
	mode_c.sort(key = lambda x:x["count"])

	s=0
	median=0
	if(count%2==1):
		for item in mode_c:
			s += item["count"]
			if(s >= count/2+1):
				median = item["_id"]
				break
	else:
		low=high=0
		for item in mode_c:
			s += item["count"]
			if(high < low):
				high = item["_id"]
				break
			if(s > count/2):
				low=high=item["_id"]
				break
			elif(s == count/2):
				low=item["_id"]
		if(high == 0):
			high = mode
		median = (low+high)/2.0

	print "Median: {}".format(median)

		
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

	
