import urllib2 as u
import config
import log,sys,json
stats=None
def loadStats():
	global stats
	try:
		stats=json.loads(open("stats.json","r").read())
		log.put("Read stats","SUCCESS")
	except Exception as e:
		log.put("Read stats","FAIL")
		sys.exit(0)
def downloadPage():
	global stats
	url=config.getConfig("base_url")
	html_data=None
	try:
		response=u.urlopen(url)
		response_headers = response.info().dict
		html_data=response.read()
		log.put("Index page downloaded","SUCCESS")
		last_update_date=response_headers["date"].strip()
		if stats['lastUpdated']!=last_update_date:
			log.put("New version available","INFO")
			return True,html_data,last_update_date
		else:
			log.put("New version not available","INFO")
			return False,None,None
	except Exception as e:
			log.put("Index page failed to download","FAIL")
			return False,None,None
def check():
	loadStats()
	return downloadPage()


	