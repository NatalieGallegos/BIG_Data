# -*- coding: utf-8 -*-

import gzip
import pymongo
import metacritic_api

def parse(filename):
  f = gzip.open(filename, 'r')
  entry = {}
  for l in f:
    l = l.strip()
    colonPos = l.find(':')
    if colonPos == -1:
      yield entry
      entry = {}
      continue
    eName = l[:colonPos]
    rest = l[colonPos+2:]
    entry[eName] = rest
  yield entry


if __name__ == "__main__":
    client = pymongo.MongoClient()
    db = client.big_data_db
    games = db.games
    #print "Sample:"
    for e in parse("./Video_Games.txt.gz"):
        game = {}
        game['title'] = e['product/title']
        game['genre'] = metacritic_api.findGame(e['product/title'])
        game['userID'] = e['review/userId']
        game['userName'] = e['review/profileName']
        game['review/score'] = e['review/score']
        game['review/text'] = e['review/text']
        games.insert_one(game).inserted_id  
        #break #just display the first entry

