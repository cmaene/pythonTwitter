import sys, oauth2, json, MapReduce
from twython import Twython

# python2.7 twitterWithTwython.py > ferguson.txt

# =============================

mr = MapReduce.MapReduce()

def preprocess():
    api_key = "rkmLMnVjGYI2H5A2lSqVXpeuP"
    api_secret = "BtoAXb56cxGVU94sjcJPcCQo28jgHwyOFTaXWTaYrtFw6kvsF4"
    access_token_key = "96202775-EZh4A4nQ1rIRrMOvFk8tuAh5rrz2VGAgy4In4CJOA"
    access_token_secret = "FggLLfhmyestPZwzHWgGxW5bbdtz850FilOZqrppEVyLQ"
    dictweets = []
    twitter = Twython(app_key=api_key, app_secret=api_secret, \
    oauth_token=access_token_key, oauth_token_secret=access_token_secret)
    tweets = twitter.search(q='ferguson')
    # narrowing search doesn't seem necessary as I get only a few tweets for any search
    #tweets = twitter.search(q='ferguson', geocode='41.878114,-87.629798,5mi', include_entities='false')
    inStatuses=json.dumps(tweets['statuses'], encoding="utf-8") # keep only 'statuses' part
    tweets= json.loads(inStatuses, encoding="utf-8") #need to reload as json to turn tweets per line
    #print(json.dumps(tweets, indent=2))
    f=open('farguson.json','w')
    for line in tweets:
	#tweet=json.loads(line)
        if 'text' in line:
    	    dic  = {} # here - this is where we create a new dictionary
	    dic['text']= line['text']
	    #dic+=', '
            dic['geo']= str(line['geo'])
	    f.write(json.dumps(dic, encoding="utf-8")+'\n')
            #f.write(json.dumps(tweets,encoding="utf-8")+'\n')
	    #dictweets.append(line['text'])
    f.close
    #return(dictweets)'''

def mapper(record):
    # key: document identifier - omit this time
    # value: text/tweet content
    value = record['text']
    words = value.split()
    for w in words:
      mr.emit_intermediate(w, 1)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))
    
    #print(count)
# =============================
if __name__ == '__main__':
  preprocess()
  inputdata = open('farguson.json','r')
  mr.execute(inputdata, mapper, reducer)
