import log,config
import sys,cron,json
from bs4 import BeautifulSoup as bs
import os
from urllib2 import urlopen, URLError, HTTPError
import ntriples as nt
os.system("rm *.nt")
os.system("rm *.bz2")
def dlfile(url):
    # Open the url
    try:
        f = urlopen(url)
        print "downloading " + url

        # Open our local file for writing
        with open(os.path.basename(url), "wb") as local_file:
            local_file.write(f.read())
        log.put("Download success "+url,"SUCCESS")
        log.put("Starting decompressing","INFO")
        os.system("bunzip2 -d "+os.path.basename(url))
        log.put("File decompressed "+url,"SUCCESS")
        print "yoo"
    #handle errors
    except HTTPError, e:
        print "HTTP Error:", e.code, url
        log.put("Download failed "+url,"ERROR")
    except URLError, e:
        print "URL Error:", e.reason, url
        log.put("Download failed "+url,"ERROR")

def updateFiles(dic):
	log.put("Downloading tracked files","INFO")
	li=config.getConfig("tracking_files") #list of files to be updated
	for l in li:
		try:
			if(cron.stats["files"][l]!=dic[l]):
				#file changed
				dlfile(config.getConfig("base_url")+l)
				log.put("Parsing "+config.getConfig("base_url")+l,"INFO")
				nt.parseURI(l.replace(".bz2",""),l.split(".")[0])
				log.put("Parsed "+config.getConfig("base_url")+l,"SUCCESS")
		except KeyError:
				dlfile(config.getConfig("base_url")+l)
				log.put("Parsing "+config.getConfig("base_url")+l,"INFO")
				nt.parseURI(l.replace(".bz2",""),l.split(".")[0])
				log.put("Parsed "+config.getConfig("base_url")+l,"SUCCESS")
	log.put("Tracked files updated","SUCCESS")
	log.put("Deleting all files from cache","INFO")
	os.system("rm -rf *.nt")
	log.put("Files deleted from cache","SUCCESS")
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
		updateFiles(dic)
		stats={}
		stats['lastUpdated']=update_date.strip()
		stats["files"]={}
		for key in dic:
			if key in config.getConfig("tracking_files"):
				stats["files"][key]=dic[key].strip()
		try:
			del stats["files"]["../"] #remove parent index
		except KeyError:
			pass
		try:
			open("stats.json","w").write(json.dumps(stats, indent=4, sort_keys=True))
			log.put("Stats updated","SUCCESS")
		except:
			log.put("Cannot update stats","FAIL")
		
		log.headPut("Finished cron-dbpedia","SUCCESS")
	else:
		#No change exit silently
		sys.exit(0)




if __name__=="__main__":
	#being executed
	log.headPut("Started cron-dbpedia","SUCCESS")
	init()