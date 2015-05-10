import gzip
import json
import sys
from pymongo import MongoClient


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


if __name__ == '__main__':
	client = MongoClient()
	db = client.cs594

	i=0
	print "inserting movies"
	db.movies.drop()
	for e in parse("../Movies_&_TV.txt.gz"):
		i += 1
		sys.stdout.write("{}\r".format(i))
		sys.stdout.flush()
		del e['review/text']
		db.movies.insert_one(e)

	i=0
	print "inserting music"
	db.music.drop()
	for e in parse("../Music.txt.gz"):
		i += 1
		sys.stdout.write("{}\r".format(i))
		sys.stdout.flush()
		del e['review/text']
		db.music.insert_one(e)

	i=0
	print "inserting books"
	db.books.drop()
	for e in parse("../Books.txt.gz"):
		i += 1
		sys.stdout.write("{}\r".format(i))
		sys.stdout.flush()
		del e['review/text']
		db.books.insert_one(e)

	i=0
	print "inserting video games"
	db.games.drop()
	for e in parse("../Video_Games.txt.gz"):
		i += 1
		sys.stdout.write("{}\r".format(i))
		sys.stdout.flush()
		del e['review/text']
		db.games.insert_one(e)