from bs4 import BeautifulSoup
import csv
import requests

print "Reading page..."
url = 'http://dallas.backpage.com/BodyRubs/im-calidreamsuper-sweet-a-relaxation-to-die-for/33572039'
#URL we are scraping from is assigned as a variable called url
r = requests.get(url)

data = r.text
soup = BeautifulSoup(data)

postTitle = soup.find_all("a", {"class":"h1link"})
postDate = soup.find_all("div", {"class":"adInfo"})
postBody = soup.find_all("div", {"class":"postingBody"})
postInfo = soup.find_all("p", {"class":"metaInfoDisplay"})

#where information is stored
posting_title = []
date = []
body = []
info = []

#parse data 
print "Parsing data..."
for html in postTitle:
	text = BeautifulSoup(str(html).strip()).get_text().encode("utf-8").replace("\n", "")
	posting_title.append(text.split("Title:"))

for html in postDate:
	text = BeautifulSoup(str(html).strip()).get_text().encode("utf-8").replace("\n", "")
	date.append(text.split("Data:"))

for html in postBody:
	text = BeautifulSoup(str(html).strip()).get_text().encode("utf-8").replace("\n", "")
	body.append(text.split("Body:"))

for html in postInfo:
	text = BeautifulSoup(str(html).strip()).get_text().encode("utf-8").replace("\n", "")
	info.append(text.split("Info:"))

#open csv file
csvfile = open('test.csv', 'wb')
writer = csv.writer(csvfile)

#clear old data in file
csvfile.truncate

#define headers
writer.writerow(["Title", "Date", "Body", "Info"])

#write information intof ile
for posting_title in zip(posting_title, date, body, info):
	writer.writerow([posting_title, date, body, info])

csvfile.close()

print "Operation completed successfully"
