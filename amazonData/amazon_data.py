import gzip
import json
import sys

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

print "Sample:"
for e in parse("../Video_Games.txt.gz"):
  print json.dumps(e, indent=4)
  break #just display the first entry

print "Count:"
i = 0
for e in parse("../Video_Games.txt.gz"):
  i += 1
  sys.stdout.write("{}\r".format(i))
  sys.stdout.flush()

print "\ndone!"


