import log,config
import sys,cron,json
from bs4 import BeautifulSoup as bs


def parseIndex(data):
	log.put("Start parsing index","INFO")
	soup=bs(data)
	pres=soup.find("pre")
	dic={}
	if pres is not None:
		aas=pres.findAll("a")
		for aaas in aas:
			if aaas is not None:
				dic[aaas.get("href").strip()]=aaas.next_sibling.split("\t")[0].strip()
		return dic
	else:
		log.put("Parsing error no pre tag","ERROR")
		return None
def init():
	status,data,update_date=cron.check()
	if status:
		#download index changed
		dic=parseIndex(data)#fetch json of data
		if dic is None:
			sys.exit(0)
		stats={}
		stats['lastUpdated']=update_date.strip()
		stats["files"]={}
		for key in dic:
			stats["files"][key]=dic[key].strip()
		del stats["files"]["../"] #remove parent index
		try:
			open("stats.json","w").write(json.dumps(stats, indent=4, sort_keys=True))
			log.put("Stats updated","SUCCESS")
		except:
			log.put("Cannot update stats","FAIL")
	else:
		#No change exit silently
		sys.exit(0)




if __name__=="__main__":
	#being executed
	init()