from multiprocessing import Pool
import json
from srmse import db
es=db.getES()
import threading
import string
import re,os,sys
files=os.listdir(".")
done=[]
db = db.getMongo()
db = db["cron-dbpedia"]
c=db.wiki_done.find({"done":{"$exists":True}})
for cc in c:
	done.append(cc["done"])
print "previous done ",c.count()
if not "result.json" in files:
	#dump the collection to get seeds
	f=db["short-abstracts_en"].find({})
	for ff in f:
		open("result.json","a").write(json.dumps(ff)+"#SPLIT#\n#SPLIT#")
	print "Dumped article list"
	
def index(d,i,db):
	idd=d["_id"]
	del d["_id"]
	try:
		es.index(index="wiki_module1",id=idd,doc_type="doc",body=d)
		print "Indexed"
		db.wiki_done.insert({"done":i},w=0)
	except:
		d["_id"]=idd
		db.wiki_done.insert({"failed":i},w=0)
	
def fetchInfoBox(url,db):
	a=db["infobox-properties_en"].find({"_id":url})
	if a.count()==0:
		return {}
	elif a.count()==1:
		return json.loads(json.dumps(a.next()["box"]).replace("#dot#",".").replace("http://dbpedia.org/resource/","http://www.wikipedia.org/wiki/"))
	else:
		return {}
		
def fetchExternalLinks(url,db):
	a=db["external-links_en"].find({"_id":url})
	if a.count()==0:
		return []
	elif a.count()==1:
		return json.loads(json.dumps(a.next()["to"]).replace("http://dbpedia.org/resource/","http://www.wikipedia.org/wiki/"))
	else:
		return []
		
def fetchPageViews(url,db):
	a=db["stats"].find({"_id":url})
	if a.count()==0:
		return 1
	elif a.count()==1:
		return a.next()["visits"]
	else:
		return 1
		
def isDisambiguate(url,db):
	a=db["disambiguations_en"].find({"_id":url})
	if a.count()==0:
		return (False,[])
	elif a.count()==1:
		return (True,a.next()["to"])
	else:
		return (False,[])
def fetchHomePage(url,db):
	a=db["homepages_en"].find({"_id":url})
	if a.count()==0:
		return None
	elif a.count()==1:
		return a.next()["website"]
	else:
		return None

def fetchImage(url,db):
	a=db["images_en"].find({"_id":url})
	if a.count()==0:
		return None
	elif a.count()==1:
		return a.next()["images"]
	else:
		return None

def getCategories(url,db):
	a=db["article-categories_en"].find({"_id":url})
	if a.count()==0:
		return (False,[],{})
	elif a.count()==1:
		k=a.next()["categories"]
		k=json.loads(json.dumps(k).replace("http://dbpedia.org/resource/Category:","http://en.wikipedia.org/wiki/Category:"))
		d={}
		i=0
		for kk in k:
			d["c_"+str(i)]=searchKey(kk)
			i+=1	
		return (True,k,d)
	else:
		return (False,[],{})
	
def getBestResolved(url,options,db):
	maxx=0
	t=None
	for op in options:
		a=db["stats"].find_one({"_id":op})
		if a is None:
			continue
		else:
			if a["visits"]>maxx:
				maxx=a["visits"]
				t=a["_id"].replace("http://dbpedia#dot#org/resource/","http://www.wikipedia.org/wiki/")
	return t

def create(d):
	try:
		d=json.loads(d)
		if d["_id"] in done:
			print "Ignore"
			return
		from srmse import db
		db=db.getMongo()
		db=db["cron-dbpedia"]
		dic={}
		dic["_id"]=d["_id"].replace("http://dbpedia#dot#org/resource/","http://www.wikipedia.org/wiki/").replace("#dot#",".")
		dic["body"]=d["summary"]
		dic["search"]=d["search"]
		dic["box"]=fetchInfoBox(d["_id"],db)
		dic["external_links"]=fetchExternalLinks(d["_id"],db)
		dic["page_views"]=fetchPageViews(d["_id"],db)
		dic["home_page"]=fetchHomePage(d["_id"],db)
		dic["image"]=fetchImage(d["_id"],db)
		k=getCategories(d["_id"],db)
		if k[0]:
			dic["categories"]=k[1]
			dic["categories_search_fields"]=k[2]
		else:
			dic["categories"]=[]
			dic["categories_search_fields"]={}
	
		dd=isDisambiguate(d["_id"],db)
		dic["isDisambiguation"]=dd[0]
		dic["resolvedTo"]=dd[1]
		if dd[0]:
			dic["bestResolved"]=getBestResolved(d["_id"],dd[1],db)
		else:
			dic["bestResolved"]=None
		ff=getRedirects(d["_id"],db)
		if ff[0]:
			dic["hasRedirects"]=True
			dic["redirects"]=ff[1]
			dic["redirects_search_fields"]=ff[2]
		else:
			dic["hasRedirects"]=False	
		index(dic,d["_id"],db)
	except Exception as e:
		print e
	
def searchKey(key):
	regex = re.compile('[%s]' % re.escape(string.punctuation))
	key=key.replace("http://dbpedia.org/resource/","").replace("http://dbpedia#dot#org/resource/","").replace("http://dbpedia.org/resource/Category:","").replace("http://en.wikipedia.org/wiki/Category:","")
	out = regex.sub(' ',key)
	r=re.compile("\s{2,}")
	out=r.sub(' ',out).strip().lower()
	return out
	
	
def getRedirects(url,db):
	a=db["redirects_en"].find({"_id":url})
	li=[]
	i=0
	dic={}
	if a.count()==0:
		return False,[],{}
	elif a.count()==1:
		fromm=a.next()["from"]
		for ff in fromm:
			li.append(ff.replace("http://dbpedia.org/resource/","http://www.wikipedia.org/wiki/"))
			dic["redirect_search_"+str(i)]=searchKey(ff)
			i+=1
	else:
		return False,[],{}
		
		
	return True,li,dic

	
f=open("result.json","r").read().split("#SPLIT#\n#SPLIT#")
print "Total Articles ",len(f)
print "Remaining ",len(f)-len(done)
p=Pool(10)
p.map(create,f)
"""
f=db["disambiguations_en"].find({})
for ff in f:
	if threading.activeCount()<200:
		threading.Thread(target=create1,args=(ff,)).start()
	else:
		while True:
			if threading.activeCount()<200:
				break

def create1(d):
	global fi
	dic={}
	dic["_id"]=d["_id"].replace("http://dbpedia#dot#org/resource/","http://www.wikipedia.org/wiki/")
	dic["body"]=d["summary"]
	dic["search"]=d["search"]
	dic["box"]=fetchInfoBox(d["_id"])
	dic["external_links"]=fetchExternalLinks(d["_id"])
	dic["page_views"]=fetchPageViews(d["_id"])
	dic["home_page"]=fetchHomePage(d["_id"])
	dd=(True,d["to"])
	dic["isDisambiguation"]=dd[0]
	dic["resolvedTo"]=dd[1]
	if dd[0]:
		dic["bestResolved"]=getBestResolved(d["_id"],dd[1])
	else:
		dic["bestResolved"]=None
	ff=getRedirects(d["_id"])
	if ff[0]:
		dic["hasRedirects"]=True
		dic["redirects"]=ff[1]
		dic["redirects_search_fields"]=ff[2]
	else:
		dic["hasRedirects"]=False		
	index(dic)
"""
