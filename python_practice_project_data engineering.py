import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

tmpfile = "dealership_temp.tmp"
logfile = "dealership_logfile.txt"
targetfile = "dealership_transformed_data.csv"

class ETL():
	def __init__(self) -> None:
		pass
	def extract_from_csv(self, file_to_process) -> pd.DataFrame:
		dataframe = pd.read_csv(file_to_process)
		return dataframe

	def extract_from_json(self, file_to_process) -> pd.DataFrame:
		dataframe = pd.read_json(file_to_process, lines=True)
		return dataframe

	def extract_from_xml(self, file_to_process) -> pd.DataFrame:
		dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
		tree: ET = ET.parse(file_to_process)
		root = tree.getroot()
		for car in root:
			car_model = car.find("car_model").text
			year_of_manufacture = car.find("year_of_manufacture").text
			price = float(car.find("price").text)
			fuel = car.find("fuel").text
			dataframe = pd.concat([dataframe, pd.DataFrame([{
				"car_model": car_model,
				"year_of_manufacture": year_of_manufacture,
				"price": price,
				"fuel": fuel
			}])], ignore_index=True)
		return dataframe

	def extract(self) -> pd.DataFrame:
		extracted_data = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])

		for csvfile in glob.glob("datasource/*.csv"):
			extracted_data = pd.concat([extracted_data, self.extract_from_csv(csvfile)], ignore_index=True)
		for jsonfile in glob.glob("datasource/*.json"):
			extracted_data = pd.concat([extracted_data, self.extract_from_json(jsonfile)], ignore_index=True)
		for xmlfile in glob.glob("datasource/*.xml"):
			extracted_data = pd.concat([extracted_data, self.extract_from_xml(xmlfile)], ignore_index=True)

		return extracted_data

	def transform(self, data) -> pd.DataFrame:
		data['price'] = round(data.price, 2)
		# data['weight'] = round(data.weight * 0.0254, 2)
		return data

	def load(self, targetfile, data_to_load) -> None:
		data_to_load.to_csv(targetfile)

def log(message) -> None:
	timestamp_format = '%Y-%h-%d-%H:%M:%S'
	now = datetime.now()
	timestamp = now.strftime(timestamp_format)
	with open(logfile, "a") as f:
		f.write(timestamp + ',' + message + '\n')

log('ETL Job started')
etl = ETL()
log("Extract phase Started")
extracted_data = etl.extract()
log("Extract phase Ended")

log("Transform phase Started")
transformed_data = etl.transform(extracted_data)
log("Transform phase Ended")

log("Load phase Started")
etl.load(targetfile,transformed_data)
log("Load phase Ended")

log("ETL Job Ended")