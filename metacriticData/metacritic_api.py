# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 23:41:46 2015

Use the get findMovie() Function to find a movie in metacritic.
This will return data about the movie, including the url. 

The function internally gets ratings for the movie as well. 

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
        print(str(review["name"]))
        print(str(review["score"]))
        print(str(review["date"]))

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
    
    if str(response.body) != 'False!':
        data = response.body
        resultSet = data["result"]
        print(str(resultSet["genre"]) + "::" + str(resultSet["cast"]) + "::" + str(resultSet["url"]) + "\n")
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
    if str(results).lower() != 'false':
        genres = results["genre"]
        print(str(results["name"]) + "\n" + str(results["genre"]) + "\n" + 
        str(results["platform"].split("\n")) )
        return genres
    #return str(results["genre"])
    
#Test statements

#genres = findMovie("Titanic")
print("Test statements")
gameGenres = findGame("Little Inferno")
#print (genres)  
#getReviews("titanic")     
print("THE END!")