bcolors={
	"HEADER" : '\033[95m',
    "OKBLUE" : '\033[94m',
    "OKGREEN" : '\033[92m',
    "WARNING" : '\033[93m',
    "FAIL" : '\033[91m',
    "ENDC" : '\033[0m',
    "BOLD" : '\033[1m',
    "UNDERLINE" : '\033[4m'
}
from time import gmtime, strftime
def put(msg,type):
	f=open("cron-dbpedia.log","a")
	print bcolors[type.upper()] + ""+"["+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"]\t["+type.strip().capitalize()+"]\t"+msg+"" + bcolors["ENDC"]
	f.write("["+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"]\t["+type.strip().capitalize()+"]\t"+msg+"\n")
	f.close()