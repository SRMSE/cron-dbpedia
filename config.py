import json,log,sys
c=None
try:
	c=json.loads(open("config.json","r").read())
	log.put("Read config","SUCCESS")
except Exception as e:
	log.put("Read config","FAIL")
	sys.exit(0)
def getConfig(key):
	try:
		return c[key]
	except KeyError as e:
		log.put(key+" not present in config","WARNING")
		return None