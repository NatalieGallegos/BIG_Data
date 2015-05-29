import math
from pymongo import MongoClient
from numpy.random import normal
import matplotlib.pyplot as plt
import time
import sys

def getScore(doc):
	return doc['review/score']

def generateScoreHistogram(collection):
	sys.stdout.write("generating histogram for {}:".format(collection.full_name))
	start = time.time()
	sys.stdout.flush()
	scores = collection.find({"review/score":{"$exists":True}},{"_id":0, 'review/score':1, 'review/userId':1})
	knownScoreList = []
	unknownScoresList = []
	for score in scores:
		if score["review/userId"] == "unknown":
			unknownScoresList.append(getScore(score))
		else:
			knownScoreList.append(getScore(score))

	totalplt = plt.hist(knownScoreList + unknownScoresList, label=['total'])
	knownplt = plt.hist(knownScoreList, label=['registered'])
	unknownplt = plt.hist(unknownScoresList, label=['anonymous'])

	plt.title("{} review score histogram".format(collection.full_name))
	plt.xlabel("Rating")
	plt.ylabel("Count")
	plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
	plt.savefig("dashboard/{}.png".format(collection.full_name), bbox_inches='tight')
	plt.close()

	sys.stdout.write(" {} seconds\n".format(time.time()-start))
	sys.stdout.flush()

	

if __name__ == '__main__':
	client = MongoClient()
	db = client.cs594

	generateScoreHistogram(db.games)
	generateScoreHistogram(db.music)
	generateScoreHistogram(db.movies)
	generateScoreHistogram(db.books)

