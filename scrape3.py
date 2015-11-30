from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
import requests
import re
import json

#Declare Functions-------------------------------------------------------------

def make_soup(url):
	r 		= requests.get(url)
	data 	= r.text
	soup 	= BeautifulSoup(data)
	return	soup

def add_geopoint_mapping_for_doc_type(index, doc_type):
	body = {}
	body[doc_type] = {"properties" : {"loc" : {"type" : "geo_point"}}}
	client.indices.put_mapping(
		index 		= index,
		doc_type 	= doc_type,
		body 		= body
	)

def location_from_url(url):
	matches = filter(lambda x: x['href']==url, locations)
	if len(matches) == 0:
		return None
	else:
		res = matches[0]['res']
		
		if res:
			return { "lat" : res.get("latitude", None) , "lon" : res.get("longitude", None) }

#End of Function Declarations--------------------------------------------------

locations = []

path_to_locations = 'C:\Python27\locations.json'

with open(path_to_locations, 'rb') as f:
	locations = filter(lambda x: x.get('res', False), json.load(f))

#Initialize elasticsearch python client, pointing at local ES instance
client = Elasticsearch([{'host' : 'localhost', 'port' : 9200}])

url="http://www.backpage.com"

#Convert city landing page into soup
soup 		= make_soup(url)
us_geo		= soup.find_all("div",class_="united-states geoBlock")[0]
city_list	= us_geo.findAll("li")

#Establish regex patterns
title_pat		= re.compile("<title>(.*?)<\/title>")
phone_num_pat	= re.compile("\(?[0-9]{3}\)?[- .]?[0-9]{3}[- .]?[0-9]{4}")
clean_text		= re.compile(r'[\s \n \t \r]+')

for city in city_list:
	
	city_name	= city.find(text=True)
	city_url	= city.find("a")["href"]
	
	#Replace spaces in city names, for indexing
	city_name 	= re.compile(r' ').sub('_', city_name)

	#Acquire information for specific city
	city_soup	= make_soup(city_url+"/adult")

	post_list	= city_soup.find_all("div", class_="cat")
	
	print "Getting information for: "+city_name
	
	for post in post_list:

		post_info = {}

		#Get post url
		post_link 			= post.find("a")["href"]
		post_info["link"]	= post_link
		
		#Get postid from link
		post_id=post_link.rsplit('/',1)[1]
		
		#Get individual post data to get information
		post_soup 	= make_soup(post_link)
		soup_text	= post_soup.getText()
		
		#Get post title
		post_info["title"] = title_pat.findall(post_soup.encode("utf-8"))[0]
		
		#Get post body and get rid of extraneous space
		temp_body 			= post_soup.find("div", class_="postingBody").getText()
		post_info["body"]	= clean_text.sub(' ', temp_body)

		#Get phone numbers; check for duplicates before storing
		temp_phone					= phone_num_pat.findall(soup_text)
		post_info["phone number"] 	= list(set(temp_phone))
		
		#Get poster's age
		age_index			= soup_text.find("Poster's age: ")
		post_info["age"]	= soup_text[(age_index+14):(age_index+16)]

		#Match the post url with the url in locations file, to get geo-location
		geo_url = "/".join(post_info["link"].split('/')[:3])
		post_info["location"] = location_from_url(geo_url + '/')

		# add_geopoint_mapping_for_doc_type(index = 'backpage', doc_type = city_name)

		#Store all post info into post id
		client.index(index = 'backpage', doc_type = city_name, id = post_id, body = post_info)