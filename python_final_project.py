from bs4 import BeautifulSoup
import html5lib
import requests
import pandas as pd

targetfile = "bank_market_cap.json"
url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
html = requests.get(url).text

html_data = BeautifulSoup(html, "html.parser")

tables = html_data.find_all('table')

for index,table in enumerate(tables):
    if ("Market cap" in str(table)):
        table_index = index

columns = []
for col in tables[table_index].tbody.find_all('th'):
	columns.append(col.text)

data = pd.DataFrame(columns=columns)
for row in tables[table_index].tbody.find_all("tr"):
    col = row.find_all("td")
    if (col != []):
        col1 = col[0].text.strip()
        col2 = col[1].text.strip()
        col3 = col[2].text.strip()
        
        data = data.append({columns[0]: col1, columns[1]: col2, columns[2]: col3}, ignore_index=True)

data.to_json(targetfile)