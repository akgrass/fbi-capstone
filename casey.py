from elasticsearch import Elasticsearch

from bs4 import BeautifulSoup

import requests

import re

import json



### Our functions ---------------------------------------------------------


def add_geopoint_mapping_for_doc_type(index = 'backpage', doc_type = 'ads'):

    """ add the mapping for the geo location """

    body = {}

    body[doc_type] = {"properties" : {"loc" : {"type" : "geo_point"}}}

    client.indices.put_mapping(

        index = index,

        doc_type = doc_type,

        body = body

    )





def location_from_url(url):

    """ get lat / lon from a list of cities """

    matches = filter(lambda x: x['href'] == url, locations)

    if len(matches) == 0:

        return None

    else:

        res = matches[0]['res']

        if res:

            return { "lat" : res.get('latitude', None), "lon" : res.get('longitude', None) }





def make_soup(url):

    """ turn a url into a soup object """

    r    = requests.get(url)

    data = r.text

    soup = BeautifulSoup(data)

    return soup



### End of function declarations -------------------------------------------------------------



# locations holds all of our geo-location dictionaries

locations         = []



# path to our JSON file that holds our geo-location data

path_to_locations = 'locations.json'



# read in the JSON that has all of our geo-location data

with open(path_to_locations, 'rb') as f:

    locations = filter(lambda x: x.get('res', False), json.load(f))



#Initialize elasticsearch python client, pointing at your local ES instance

client = Elasticsearch([{'host' : 'localhost', 'port' : 9200}])



url = "https://urldefense.proofpoint.com/v2/url?u=http-3A__www.backpage.com&d=AwIGAg&c=-dg2m7zWuuDZ0MUcV7Sdqw&r=iKe4P6ksP7onjRkQbFbBNNNfKCx5vEDpvbANxqTV0n8&m=HBhbJHhxW6kptQLVnwrHVYZfyMBM-PRGIrfGmNWfhBs&s=PHy30Zv6IfFWhup8-wX82oEdV9KNzYDe3Q_ukK_devo&e= "


#Convert city landing page into soup

soup      = make_soup(url)

us_geo    = soup.find_all("div",class_="united-states geoBlock")[0]

city_list = us_geo.findAll("li")



#Establish regex patterns

title_pat     = re.compile("<title>(.*?)<\/title>")

phone_num_pat = re.compile("\(?[0-9]{3}\)?[- .]?[0-9]{3}[- .]?[0-9]{4}")

clean_text    = re.compile(r'[\s \n \t \r]+') # extra spaces, newlines, tabs, and carriage returns



for city in city_list:

    city_name = city.find(text=True)

    city_url  = city.find("a")["href"]

    print "Getting information for: "+city_name



    #Replace spaces in city names, for indexing

    city_name = re.compile(r' ').sub('_', city_name)



    #Acquire information for specific city

    city_soup = make_soup(city_url+"/adult")

    post_list = city_soup.find_all("div", class_="cat")



    for post in post_list:

        post_info = {}



        # grab post url

        post_info["link"] = post.find("a")["href"]



        #Get post_id from link

        post_id = post_info["link"].rsplit('/',1)[1]



        #Get individual post data to get information

        post_soup = make_soup(post_info["link"])

        soup_text = post_soup.getText()



        #Get post title

        post_info["title"] = title_pat.findall(post_soup.encode("utf-8"))[0]

        #Get post body and get rid of extra spaces, newlines, tabs, and carriage returns

        #and replace them with a single white space

        temp_body         = post_soup.find("div",class_="postingBody").getText()

        post_info["body"] = clean_text.sub(' ', temp_body)



        #Get phone numbers

        temp_phone = phone_num_pat.findall(soup_text)

        """ Get rid of duplicates. A Set can only have unique values.

            Once duplicates are gone, turn it back into a list and

            add it to your dictionary. """

        post_info["phone number"] = list(set(temp_phone))



        #Get poster's age

        age_index        = soup_text.find("Poster's age: ")

        post_info["age"] = soup_text[(age_index+14):(age_index+16)]



        # make the url that will help us find the geo-location

        geo_url = "/".join(post_info["link"].split('/')[:3])



        # get the geo-location and add it to our dictionary

        post_info['loc'] = location_from_url(geo_url + '/')



        # add Elasticsearch mapping for this ad

        add_geopoint_mapping_for_doc_type(index = 'backpage', doc_type = city_name)



        # add ad to the Elasticsearch index

        client.index(index = 'backpage', doc_type = city_name, id = post_id, body = post_info)
