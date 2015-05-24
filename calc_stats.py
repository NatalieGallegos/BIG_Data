from pymongo import MongoClient
from collections import Counter

if __name__ == '__main__':
	client = MongoClient()
	db = client.cs594

	print "Finding mean, median, mode, and stdev..."
	for c in db.categories.find():
		p_id = c["product/productId"]
		#print p_id
		category = c["product/category"]
		#print category

		results = []
		n = 0
		cursor = None
		
		if category=="Books":
			cursor = db.books.find({"product/productId": p_id}).skip(0).limit(10).sort("review/score")
		elif category=="Movies & TV":
			cursor = db.movies.find({"product/productId": p_id}).skip(0).limit(10).sort("review/score")
		elif category=="Music":
			cursor = db.music.find({"product/productId": p_id}).skip(0).limit(10).sort("review/score")
		elif category=="Video Games":
			cursor = db.movies.find({"product/productId": p_id}).skip(0).limit(10).sort("review/score")
		
		if not cursor is None:
			for result in cursor:
				results.append(float(result["review/score"]))
				n+=1

		#print results
		mean = sum(results)/n
		median = 0.0
		if (len(results)%2==1):
			median = results[((len(results)+1)/2)-1]
		else:
			median = float(sum(results[(len(results)/2)-1:(len(results)/2)+1]))/2.0
		counts = Counter(results)
		mode = counts.most_common(1)[0][0]
		stdev = 0.0
		print p_id, category, mean, median, mode, stdev
