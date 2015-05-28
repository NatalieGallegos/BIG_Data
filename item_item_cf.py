import pymongo


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
            
def fill_with_reviews(collection, products_dict, limit):
    all_users = {}
    results = collection.aggregate([{'$match':{"review/score":\
    {'$exists': True, '$ne': None},\
    "review/userId":{'$ne':"unknown"}}},\
    {'$group':{'_id':"$product/productId", "scores":{'$push':"$review/score"},\
    "users":{'$push':"$review/userId"}}},{'$limit': limit}])
    #add reviews and user pairs to each product_id
    for result in results:
        product_id = result['_id']
        scores = result['scores']
        users = result['users']
        for i in range(len(scores)):
            user = users[i]
            score = scores[i]
            products_dict[product_id][user] = score
            all_users[user] = 1
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

if __name__ == '__main__':
    client = pymongo.MongoClient()
    db = client.cs594
    
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


    pipeline = [{"$match": {"review/score": {"$exists": "true", "$ne": "null"}}},\
    {"$group": {"_id": "$product/productId", "review/score": {"$push": "$review/score"}, "review/userId": {"$push": "$review/userId"}}},\
    {"$limit": 10}\
    ]
    
    cursor = db.command('aggregate', 'games', pipeline=pipeline)
    for result in cursor:
        print result

    #Testing fill_reviews - careful!
    fill_with_reviews(db.games, game_products_dict, len(game_products_dict))
    sample = game_products_dict.keys()[0]
    print(str(sample) + str(game_products_dict[sample]))



