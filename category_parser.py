import gzip
import json
import sys
import re
from pymongo import MongoClient


def parse_titles(filename):
	f = gzip.open(filename, 'r')
	for l in f:
		l = l.strip()
		tokens = l.split(" ", 1)
		entry = {}
		if (len(tokens)==2):
			title = tokens[1].decode('utf-8', 'ignore').encode('utf-8')
			entry["product/productId"]=tokens[0]
			entry["product/title"]=title
		yield entry


def parse_categories(filename):
	f = gzip.open(filename, 'r')
	curr_product_id = ""
	category = ""
	for l in f:
		if len(l)==11 and (not "," in l):
			if len(curr_product_id)==10 and (category != ""):
				entry = {}
				entry["product/productId"]=curr_product_id
				entry["product/category"]=category
				yield entry
			category = ""
			l = l.strip()
			curr_product_id = l
		elif re.match(r'[ \t]', l):
			l = l.strip()
			category = l.split(",")[0]

if __name__ == '__main__':
	client = MongoClient()
	db = client.cs594
	'''
	i=0
	print "populating titles"
	db.titles.drop()
	for e in parse_titles("../titles.txt.gz"):
		i += 1
		sys.stdout.write("{}\r".format(i))
		sys.stdout.flush()
		db.titles.insert_one(e)
		#print e.items()
	'''
	i=0
	print "reading categories"
	db.categories.drop()
	for e in parse_categories("../categories.txt.gz"):
		i += 1
		sys.stdout.write("{}\r".format(i))
		sys.stdout.flush()
		db.categories.insert_one(e)
		#print e.items()