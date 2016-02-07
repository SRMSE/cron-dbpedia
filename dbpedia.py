import log,config
import sys,cron,json



def init():
	status=cron.check()
	if status:
		#download index changed
		pass
	else:
		#No change exit silently
		sys.exit(0)




if __name__=="__main__":
	#being executed
	init()