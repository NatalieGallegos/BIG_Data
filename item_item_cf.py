import pymongo
from math import sqrt

def fill_user_reviews(collection, products_dict):
    all_users = {}
    for key in products_dict:
        result = collection.find({'product/productId': key})
        userID = result['review/userId']
        review_score = result['review/score']
        if userID != 'unknown':
            products_dict[userID] = review_score
            all_users[userID] = review_score
    
    for users in products_dict.values():
        for user in all_users:
            if user not in users:
                users[user] = 0.0
            
def fill_with_reviews(collection, item_prefs, limit):
    all_users = {}
    results = collection.aggregate([{'$match':{"review/score":\
    {'$exists': True, '$ne': None},\
    "review/userId":{'$ne':"unknown"}}},\
    {'$group':{'_id':"$product/productId", "scores":{'$push':"$review/score"},\
    "users":{'$push':"$review/userId"}}},{'$limit': limit}])
    #add reviews and user pairs to each product_id
    #item_prefs = {}
    for result in results:
        product_id = result['_id']
        #print product_id
        scores = result['scores']
        #print scores
        users = result['users']
        #print users
        '''
        for i in range(len(scores)):
            user = users[i]
            score = scores[i]
            products_dict[product_id][user] = score
            all_users[user] = 1
        '''
        user_scores_dict = {}
        for i in range(len(scores)):
            user_scores_dict[users[i].encode('ascii')]=scores[i]
        #print user_scores_dict

        item_prefs[product_id.encode('ascii')] = user_scores_dict
    
    #print item_prefs

    #fill in missing users with 0 as their reviews
    #for users in products_dict.values():
     #   for user in all_users:
      #      if user not in users:
       #         users[user] = 0.0
            
    


def get_products(collection, products_dict):
    
    #db.games.aggregate([{$match:{"review/score":{$exists:true, $ne:null}}},{$group:{_id:"$product/productId", "review/score":{$push:"$review/score"}, "review/userId":{$push:"$review/userId"}}},{$limit:10}])
    
    '''
    unique_p = []

    if (collection.count() > 10000000):
        db.subset.drop()
        for data in collection.find().limit(500000):
            db.subset.insert_one(data)
        unique_p = list(db.subset.distinct("product/productId"))
    else:
        unique_p = list(collection.distinct("product/productId"))

    db.subset.drop()
    
    for product in unique_p:
        products_dict[product] = {}
    '''

###################################################################
################ COLLABORATIVE FILTER SECTION #####################
###################################################################

#Returns a distance-base similarity score for person1 and person2

def sim_distance(prefs, item1, item2):
    #Get the list of shared_items
    si = {}
    for user in prefs[item1]:
        if user in prefs[item2]:
            si[user] = 1

    #if they have no rating in common, return 0
    if len(si) == 0: 
        return 0

    #Add up the squares of all differences
    sum_of_squares = sum([pow(prefs[item1][user]-prefs[item2][user],2) for user in prefs[item1] if user in prefs[item2]])

    return 1 / (1 + sum_of_squares)


#Returns the Pearson correlation coefficient for p1 and p2 
def sim_pearson(prefs,p1,p2):
    #Get the list of mutually rated items
    si = {}
    for item in prefs[p1]:
        if item in prefs[p2]: 
            si[item] = 1

    #if they are no rating in common, return 0
    if len(si) == 0:
        return 0

    #sum calculations
    n = len(si)

    #sum of all preferences
    sum1 = sum([prefs[p1][it] for it in si])
    sum2 = sum([prefs[p2][it] for it in si])

    #Sum of the squares
    sum1Sq = sum([pow(prefs[p1][it],2) for it in si])
    sum2Sq = sum([pow(prefs[p2][it],2) for it in si])

    #Sum of the products
    pSum = sum([prefs[p1][it] * prefs[p2][it] for it in si])

    #Calculate r (Pearson score)
    num = pSum - (sum1 * sum2/n)
    den = sqrt((sum1Sq - pow(sum1,2)/n) * (sum2Sq - pow(sum2,2)/n))
    if den == 0:
        return 0

    r = num/den

    return r

#Returns the best matches for person from the prefs dictionary
#Number of the results and similiraty function are optional params.
def top_matches(prefs,item,n=5,similarity=sim_pearson):
    scores = [(similarity(prefs,item,other),other)
                for other in prefs if other != item]
    scores.sort()
    scores.reverse()
    return scores[0:n]

#Create a dictionary of items showing which other items they are most similar to.

def calculate_sim_items(prefs,n=10):
    result = {}

    c=0
    for item in prefs:
        #Status updates for large datasets
        c+=1
        if c%100==0:
            print "%d / %d" % (c, len(prefs))
        #Find the most similar items to this one
        scores = top_matches(prefs,item,n=n,similarity=sim_distance)
        result[item] = scores
    return result

def get_recommended_items(user_prefs, item_match, user):
    user_ratings = user_prefs[user]
    scores = {}
    total_sim = {}

    #loop over items rated by this user
    for (item, rating) in user_ratings.items():

        #Loop over items similar to this one
        for (similarity, item2) in item_match[item]:

            #Ignore if this user has already rated this item
            if item2 in user_ratings:
                continue
            #Weighted sum of rating times similarity
            scores.setdefault(item2,0)
            scores[item2] += similarity * rating
            #Sum of all the similarities
            total_sim.setdefault(item2,0)
            total_sim[item2]+=similarity

    #Divide each total score by total weighting to get an average
    rankings = [(score/total_sim[item],item) for item,score in scores.items()]

    #Return the rankings from highest to lowest
    rankings.sort()
    rankings.reverse()
    return rankings

#Function to transform Item, person - > Person, item
def transform_prefs(prefs):
    results = {}
    for item in prefs:
        for person in prefs[item]:
            results.setdefault(person,{})

            #Flip item and person
            results[person][item] = prefs[item][person]
    return results

###################################################################
############################# MAIN ################################
###################################################################

if __name__ == '__main__':
    client = pymongo.MongoClient()
    db = client.cs594
    
    game_item_prefs = {}
    
    game_products_dict = {}
    #music_products_dict = {}
    #movie_products_dict = {}
    #book_products_dict = {}

    get_products(db.games, game_products_dict)
    #get_products(db.music, music_products_dict)
    #get_products(db.movies, movie_products_dict)
    #get_products(db.books, book_products_dict)    
    #fill_user_reviews(db.games, game_products_dict)

    #print "Games unique ids: ", len(game_products_dict)
    #print "Music unique ids: ", len(music_products_dict)
    #print "Movies unique ids: ", len(movie_products_dict)
    #print "Books unique ids: ", len(book_products_dict)

    '''
    pipeline = [{"$match": {"review/score": {"$exists": "true", "$ne": "null"}}},\
    {"$group": {"_id": "$product/productId", "review/score": {"$push": "$review/score"}, "review/userId": {"$push": "$review/userId"}}},\
    {"$limit": 10}\
    ]
    
    cursor = db.command('aggregate', 'games', pipeline=pipeline)
    for result in cursor:
        print result
    '''

    #Testing fill_reviews - careful!
    fill_with_reviews(db.games, game_item_prefs, 500)
    #sample = game_item_prefs.keys()[0]
    #print(str(sample) + str(game_item_prefs[sample]))
    for i in range(0, 5):
        sample = game_item_prefs.keys()[i]
        print(str(sample) + ": " + str(game_item_prefs[sample])+ "\n")

    user_prefs= transform_prefs(game_item_prefs)
    print "Transforming...\n"

    for i in range(0, 5):
        sample = user_prefs.keys()[i]
        print(str(sample) + ": " + str(user_prefs[sample])+ "\n")

    item_match = calculate_sim_items(game_item_prefs, 5)
    #print item_match

    rankings = get_recommended_items(user_prefs, item_match, user_prefs.keys()[0])
    
    print "\nRecommended items:"
    for i in range(len(rankings)):
        product_id = rankings[i][1]
        titles = db.titles.find_one({"product/productId": product_id})
        #print titles
        print (titles["product/title"]+": "+str(rankings[i][0]))