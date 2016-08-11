import urllib2 as u
from bs4 import BeautifulSoup as bs
from srmse import db
import json
mongo=db.getMongo()
dbpedia=mongo["cron-dbpedia"]
def dictify(ul):
    result = {}
    for li in ul.find_all("li", recursive=False):
        key = next(li.stripped_strings)
        ul = li.find("ul")
        if ul:
            result[key] = dictify(ul)
        else:
            result[key] = None
    return result


#html=u.urlopen("http://mappings.dbpedia.org/server/ontology/classes/").read()
#soup=bs(html)
#ul = soup.body.ul
#from pprint import pprint
#pprint(dictify(ul), width=1)
dic=eval(open("db.json","r").read())
dbpedia.classes.insert(dic)
