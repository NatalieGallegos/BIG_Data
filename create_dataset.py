import gzip
import json
import unirest


def findMovieGenre(movieTitle):
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
    result = response.body["result"]
    if str(result) != 'False':
        genre = result["genre"]
        return genre.split('\n')
    return "N/A"



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
    last_name = ""
    for e in parse("../Movies_&_TV.txt.gz"):
        if(e['product/title'] == last_name):
            continue
        last_name = e['product/title']
        title = e["product/title"].split('[', 1)[0].split('(', 1)[0].split('-', 1)[0].strip()
        genre = findMovieGenre(title)
        e['genre'] = genre
        e['review/text'] = ""
        print json.dumps(e, indent=4)
        print '\n'
    # print findMovieGenre("titanic")

