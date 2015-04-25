# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 23:41:46 2015

Use the findGame(), findMovie() Function to find a game/movie in metacritic.
This will return data about the movie, including the url. 

The function internally gets ratings for the game/movie as well. 

"""
import unirest


def getReviews(url):
# These code snippets use an open-source library.
    response = unirest.get("https://byroredux-metacritic.p.mashape.com/user-reviews?page_count=all&url=http%3A%2F%2Fwww.metacritic.com%2Fmovie%2F" + url,
      headers={
        "X-Mashape-Key": "PAz5rLbjpTmshXARxUBogiGCzZKhp1Q3K0pjsnPGIvR2ZaIpxY",
        "Accept": "application/json"
      }
    )
    data = response.body
    reviewSet = data["reviews"]
    for review in reviewSet:
    #print( "This is a review:\t" + str(review))
        print("user: " + str(review["name"]))
        print("rating: " + str(review["score"]))
        print("date: " + str(review["date"]) + "\n")

def findMovie(movieTitle):
    # These code snippets use an open-source library. http://unirest.io/python
    response = unirest.post("https://byroredux-metacritic.p.mashape.com/find/movie",
      headers={
        "X-Mashape-Key": "PAz5rLbjpTmshXARxUBogiGCzZKhp1Q3K0pjsnPGIvR2ZaIpxY",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
      },
      params={
        "retry": 4,
        "title": movieTitle
      }
    )
    genres = ["N/A"]
    data = response.body
    resultSet = data["result"]
    if str(resultSet).lower() != 'false':
        print(str(resultSet["genre"]) + "::" + str(resultSet["cast"]) + "::" + 
        str(resultSet["url"]) + "\n")
        splitURL = resultSet["url"].split("/")
        searchName = splitURL[-1]
        getReviews(searchName)
        genresSet = resultSet["genre"]
        genres = genresSet.split("\n")
        counter = 0
        while counter < len(genres):
            genres[counter] = str(genres[counter])
            counter += 1
        return genres
    else :
        print("No Match Found")
        return genres
    
def findGame(gameTitle):   
    response = unirest.post("https://byroredux-metacritic.p.mashape.com/find/game",
      headers={
        "X-Mashape-Key": "PAz5rLbjpTmshXARxUBogiGCzZKhp1Q3K0pjsnPGIvR2ZaIpxY",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
      },
      params={
        "platform": 1,
        "retry": 4,
        "title": gameTitle
      }
    )
    data = response.body
    results = data["result"]
    genre = ["N/A"]
    if str(results).lower() != 'false':
        genres = results["genre"]
        print(str(results["name"]) + "\n" + str(results["genre"]) + "\n" + 
        str(results["platform"].split("\n")) )
        splitURL = results["url"].split("/")
        searchName = splitURL[-1]
        getReviews(searchName)
        return genres
    else:
        print("No Match Fouond")
        return genre
    
#Test statements
print("Test statements")
genres = findMovie("skjhnewrerdc")
print ("genres: " + str(genres) )
gameGenres = findGame("Mortal Kombat") 
#getReviews("titanic")     
print("THE END!")

