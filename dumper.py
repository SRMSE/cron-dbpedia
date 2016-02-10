from pymongo import MongoClient
import config,urllib2
client = MongoClient(config.getConfig("mongo_url"))
db = client[config.getConfig("mongo_db")]
dic={}
data={}
last_first=None
def searchKey(key):
	key=key.replace("_"," ").replace("http://dbpedia.org/resource/","").replace("(","").replace(")","").lower()
	return key
def insert(col,first,second,third):
	global data,dic,last_first
	first=urllib2.unquote(first)
	second=urllib2.unquote(second)
	third=urllib2.unquote(third)
	if col=="homepages_en":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
		collection.insert({"wiki":first,"website":third,"search":searchKey(first)},w=0)
	elif col=="disambiguations_en":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
			last_first=first
		if first!=last_first:
			collection.insert({"wiki":last_first,"to":data[last_first]["to"],"search":searchKey(last_first)},w=0)
			last_first=first
		if first not in data:
			data[first]={}
			data[first]["to"]=[]
		data[first]["to"].append(third)
	elif col=="external-links_en":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
			last_first=first
		if first!=last_first:
			collection.insert({"wiki":last_first,"to":data[last_first]["to"],"search":searchKey(last_first)},w=0)
			last_first=first
		if first not in data:
			data[first]={}
			data[first]["to"]=[]
		data[first]["to"].append(third)
	elif col=="images_en.nt":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
			last_first=first
		if first!=last_first:
			collection.insert({"wiki":last_first,"images":data[last_first]["to"],"search":searchKey(last_first)},w=0)
			last_first=first
		if first not in data:
			data[first]={}
			data[first]["to"]=[]
		data[first]["to"].append(third)
	elif col=="short-abstracts_en":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
		collection.insert({"wiki":first,"summary":third,"$set":{"search":searchKey(first)}},w=0)
	else:
		collection = db[col]
		collection.insert({"first":first,"second":second,"third":third},w=0)