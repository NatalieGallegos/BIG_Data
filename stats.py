import pymongo

class Stats:
    client = pymongo.MongoClient()
    db = client.cs594
    moviesMean = 0
    gamesMean = 0
    booksMean = 0
    musicMean = 0
    
    #create a single method for each?
    def getMovieMeans(self):
        movies = Stats.db.movies
        movieRatings = 0
        count = 0
        for movie in movies.find({'review/score' : {'$exists' : True}}):
            if movie['review/score'] is not None:
                movieRatings += float(movie['review/score'])
                count += 1
        return movieRatings / count
    
    def getMeans(self):
        #find mean for video games
        games = Stats.db.games
        gameRatings = 0
        count = 0
        for game in games.find({'review/score' : {'$exists' : True}}):
            if game['review/score'] is not None:
                gameRatings += float(game['review/score'])
                count += 1
        Stats.gamesMean = gameRatings / count
        #find mean for movies
        movies = Stats.db.movies
        movieRatings = 0
        count = 0
        for movie in movies.find({'review/score' : {'$exists' : True}}):
            if movie['review/score'] is not None:
                movieRatings += float(movie['review/score'])
                count += 1
        Stats.moviesMean = movieRatings / count
        #find mean for music
        music = Stats.db.music
        musicRatings = 0
        count = 0
        for entry in music.find({'review/score' : {'$exists' : True}}):
            if entry['review/score'] is not None:
                musicRatings += float(entry['review/score'])
                count += 1
        Stats.musicMean = musicRatings / count
        #find mean for books
        books = Stats.db.books
        bookRatings = 0
        count = 0
        for book in books.find({'review/score' : {'$exists' : True}}):
            if book['review/score'] is not None:
                bookRatings += float(book['review/score'])
                count += 1
        Stats.booksMean = bookRatings / count
        
        
       
       
       
