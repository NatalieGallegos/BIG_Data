import pymongo

def get_products(collection, products_dict):
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

if __name__ == '__main__':
    client = pymongo.MongoClient()
    db = client.cs594
    
    game_products_dict = {}
    music_products_dict = {}
    movie_products_dict = {}
    book_products_dict = {}
    
    get_products(db.games, game_products_dict)
    get_products(db.music, music_products_dict)
    get_products(db.movies, movie_products_dict)
    get_products(db.books, book_products_dict)

    print "Games unique ids: ", len(game_products_dict)
    print "Music unique ids: ", len(music_products_dict)
    print "Movies unique ids: ", len(movie_products_dict)
    print "Books unique ids: ", len(book_products_dict)