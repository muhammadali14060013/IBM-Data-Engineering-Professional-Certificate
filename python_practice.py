from bs4 import BeautifulSoup
import requests

url = "http://www.ibm.com"
data =requests.get(url).text

soup = BeautifulSoup(data, 'html.parser')

# print(soup.prettify())

for link in soup.find_all('img'):
	print(link.get('src'))