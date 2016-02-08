from time import gmtime, strftime
import json

bcolors={
	"HEADER" : '\033[95m',
    "INFO" : '\033[94m',
    "SUCCESS" : '\033[92m',
    "WARNING" : '\033[93m',
    "FAIL" : '\033[91m',
    "ENDC" : '\033[0m',
    "BOLD" : '\033[1m',
    "UNDERLINE" : '\033[4m'
}
config=json.loads(open("config.json","r").read())

def put(msg,type):
	f=open(config["log_file"],"a")
	print bcolors[type.upper()] + ""+"["+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"]\t["+type.strip().capitalize()+"]\t"+msg+"" + bcolors["ENDC"]
	f.write("["+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"]\t["+type.strip().capitalize()+"]\t"+msg+"\n")
	f.close()
def headPut(msg,type):
	f=open(config["head_log_file"],"a")
	print bcolors[type.upper()] + ""+"["+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"]\t["+type.strip().capitalize()+"]\t"+msg+"" + bcolors["ENDC"]
	f.write("["+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"]\t["+type.strip().capitalize()+"]\t"+msg+"\n")
	f.close()