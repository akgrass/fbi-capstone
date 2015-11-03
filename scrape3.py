from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
import requests
import re
import json

#Initialize elasticsearch python client, pointing at your local ES instance
client = Elasticsearch([{'host' : 'localhost', 'port' : 9200}])

url="http://www.backpage.com"

#Convert city landing page into soup
r=requests.get(url)
data=r.text
soup=BeautifulSoup(data)

usgeo=soup.find_all("div",class_="united-states geoBlock")[0]
citylist=usgeo.findAll("li")

#Establish regex patterns
titlepat=re.compile("<title>(.*?)<\/title>")
phonenumpat=re.compile("\(?[0-9]{3}\)?[- .]?[0-9]{3}[- .]?[0-9]{4}")

for city in citylist[24:424]:
	cityname=city.find(text=True)
	cityurl=city.find("a")["href"]
	print "Getting information for: "+cityname

	#Replace spaces in city names, for indexing
	if len(re.findall(r'\w+',cityname)):
		cityname=cityname.replace(" ","_")

	#Acquire information for specific city
	r=requests.get(cityurl+"/adult")
	citydata=r.text
	citysoup=BeautifulSoup(citydata)

	postlist=citysoup.find_all("div", class_="cat")
	
	for post in postlist:
		
		postinfo={}
		postlink=post.find("a")["href"]
		postinfo["link"]=postlink
		
		#Get postid from link
		postid=postlink.rsplit('/',1)[1]
		
		#Get individual post data to get information
		r=requests.get(postlink)
		postdata=r.text
		postsoup=BeautifulSoup(postdata)
		souptext=postsoup.getText()
		
		#Get post title
		posttitle=titlepat.findall(postsoup.encode("utf-8"))[0]
		postinfo["title"]=posttitle
		
		#Get post body; COME BACK AND GET RID OF WHITE SPACE
		postbody=postsoup.find("div",class_="postingBody").getText()
		postinfo["body"]=postbody
		
		#Get phone numbers; COME BACK AND GET RID OF DUPLICATES
		numparse=phonenumpat.findall(souptext)
		postinfo["phone number"]=numparse
		
		#Get poster's age
		ageindex=souptext.find("Poster's age: ")
		posterage=souptext[(ageindex+14):(ageindex+16)]
		postinfo["age"]=posterage
		
		#Store all post info into post id
		client.index(index='backpage', doc_type=cityname, id=postid, body=postinfo)