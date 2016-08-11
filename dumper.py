from pymongo import MongoClient
import config,urllib2
client = MongoClient(config.getConfig("mongo_url"))
db = client[config.getConfig("mongo_db")]
dic={}
redirects_dic={}
data={}
last_first=None
def createKey(key):
	return key.replace(".","#dot#")
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
		collection.insert({"_id":createKey(first),"website":third,"search":searchKey(first)},w=0)
	elif col=="disambiguations_en":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
			last_first=first
		if first!=last_first:
			collection.insert({"_id":createKey(last_first),"to":data[last_first]["to"],"search":searchKey(last_first)},w=0)
			del data[last_first]
			last_first=first
		if first not in data:
			data[first]={}
			data[first]["to"]=[]
		data[first]["to"].append(third)
	elif col=="article-categories_en":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			dic[col]=True
			last_first=first
		if first!=last_first:
			collection.insert({"_id":createKey(last_first),"categories":data[last_first]["categories"]},w=0)
			del data[last_first]
			last_first=first
		if first not in data:
			data[first]={}
			data[first]["categories"]=[]
		data[first]["categories"].append(third)
	elif col=="external-links_en":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
			last_first=first
		if first!=last_first:
			collection.insert({"_id":createKey(last_first),"to":data[last_first]["to"],"search":searchKey(last_first)},w=0)
			del data[last_first]
			last_first=first
		if first not in data:
			data[first]={}
			data[first]["to"]=[]
		data[first]["to"].append(third)
	elif col=="images_en":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
			last_first=first
		if first!=last_first:
			collection.insert({"_id":createKey(last_first),"images":data[last_first]["to"],"search":searchKey(last_first),"hasType":second},w=0)
			del data[last_first]
			last_first=first
		if first not in data:
			data[first]={}
			data[first]["to"]=[]
		data[first]["to"].append(third)
	elif col=="infobox-properties_en":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
			last_first=first
		if first!=last_first:
			collection.insert({"_id":createKey(last_first),"box":data[last_first]["box"],"search":searchKey(last_first)},w=0)
			del data[last_first]
			last_first=first
		if first not in data:
			data[first]={}
			data[first]["box"]={}
		data[first]["box"][second.replace("http://dbpedia.org/property/","").replace(".","#dot#")]=third.replace("http://dbpedia.org/property/","").replace(".","#dot#")		
	elif col=="short-abstracts_en":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
		collection.insert({"_id":createKey(first),"summary":third,"search":searchKey(first)},w=0)
	elif col=="redirects_en":
		collection = db[col]
		if col not in dic:
			collection.drop()
			dic[col]=True
		if third in redirects_dic:
			redirects_dic[third].append(first)
		else:
			redirects_dic[third]=[]
			redirects_dic[third].append(first)
		#collection.insert({"from":createKey(first),"to":third},w=0)
	elif col=="instance-types-transitive_en":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
			last_first=first
		if first!=last_first:
			collection.insert({"_id":createKey(last_first),"w3c":data[last_first]["w3c"],"sc":data[last_first]["sc"],"dbc":data[last_first]["dbc"],"wikiId":data[last_first]["wikiId"],"search":searchKey(last_first)},w=0)
			del data[last_first]
			last_first=first
		#print first
		if first not in data:
			data[first]={}
			data[first]["dbc"]=[]
			data[first]["sc"]=[]
			data[first]["w3c"]=[]
			data[first]["wikiId"]=""
		if "www.wikidata.org/entity/" in third:
			#wikidata id
			data[first]["wikiId"]=third
		elif "dbpedia.org/ontology/" in third:
			data[first]["dbc"].append(third)
		elif "http://www.w3.org/" in third:
			data[first]["w3c"].append(third)
		elif "http://schema.org" in third:
			data[first]["sc"].append(third)
def dumpRedirects():
	"""
		The redirects file does not have same articles in order.
	
	"""
	collection = db["redirects_en"]
	for key in redirects_dic:
		collection.insert({"from":redirects_dic[key],"_id":createKey(key)},w=0)
