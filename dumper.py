from pymongo import MongoClient
import config
client = MongoClient(config.getConfig("mongo_url"))
db = client[config.getConfig("mongo_db")]
dic={}
def insert(col,first,second,third):
	if col=="homepages_en":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
		collection.insert({"wiki":first,"website":third,"search":first.replace("_"," ")},w=0)
	elif col=="disambiguations_en":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
		collection.update({"wiki":first}, {'$push': {'to': third},"$set":{"search":first.replace("_"," ")}}, True)
	elif col=="external-links_en":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
		collection.update({"wiki":first}, {'$push': {'to': third},"$set":{"search":first.replace("_"," ")}}, True)
	elif col=="images_en.nt":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
		collection.update({"wiki":first}, {'$push': {'images': third},"$set":{"search":first.replace("_"," ")}}, True)
	elif col=="short-abstracts_en":
		collection = db[col]
		if col not in dic:
			collection.drop() 
			collection.create_index([('search', "text")], default_language='english')
			dic[col]=True
		collection.insert({"wiki":first,"summary":third,"$set":{"search":first.replace("_"," ")}},w=0)
	else:
		collection = db[col]
		collection.insert({"first":first,"second":second,"third":third},w=0)