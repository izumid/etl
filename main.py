import md_sqlutil.postgres as postgres
import configparser
import os
import pandas as pd
from datetime import datetime
import re
import json


def data_read(path_relative,filename,feather_file,debug):

	na_values = ['', ' ', 'NA', 'N/A', 'na', 'n/a', 'null', 'NULL', 'none', 'None', 'NaN', 'nan', 'NAN', 'NaT', 'nat']
	path_absolute = os.path.join(path_relative.replace("custom",os.getlogin()), filename)
	if '1' in feather_file: dataframe = pd.read_feather(path_absolute)
	else: dataframe = pd.read_csv(path_absolute,dtype=str,sep=";",na_values=na_values,keep_default_na=True,index_col=None,parse_dates=None)
	postgres.debug_code(debug,"dataframe header",dataframe.head())
	
	return(dataframe)


def try_cast_date(x):
	after_trim_pattern = r"\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b"
	brazilian_pattern = r"\b\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}\b"

	if isinstance(x, str) and not bool(re.search(brazilian_pattern, x)) and not bool(re.search(after_trim_pattern, x)):
		x = re.sub(r'[^A-Za-z0-9/]+', '', x)
		try: x = datetime.strptime(x, "%d/%m/%Y")
		except: x = None

	return x


def main():
	config = configparser.RawConfigParser()
	config.read(os.path.join(os.getcwd(),r"config/config.ini"))
	debug = config["STATUS"]["debug"]
	database =config["DATABASE"]["database"]
	control_data_field = config["DATASET"]["control_data_field"]
	column_to_date = config["DATASET"]["column_to_date"]
	usp_name = config["PROCEDURE"]["name"]

	with open(os.path.join(os.getcwd(),r"config/config.json")) as jsf: config_json = json.load(jsf)

	df = data_read(
		path_relative=config["BASEFILE"]["path_in"]
		,filename=config["BASEFILE"]["list_name_file"]
		,feather_file=config["BASEFILE"]["feather_file"]
		,debug=debug
	)
	
	if control_data_field:
		if '1' in config["DATASET"]["column_trim"]:
			postgres.debug_code(debug,"Trim and ajust to data")
			df[control_data_field] = df[control_data_field].apply(lambda x: try_cast_date(x))

		if '1' in column_to_date:
			postgres.debug_code(debug,"Cast to date")
			df[control_data_field] = pd.to_datetime(df[control_data_field], errors='coerce')
			df[control_data_field] = df[control_data_field].dt.strftime("%Y-%m-%d")

	df = df.where(df.notnull(), None)
	
	postgres.debug_code(debug,"dataframe header",df.head())

	conn = postgres.connection(db_config=config_json,debug=debug)
	
	postgres.debug_code(debug,"connection variable",conn)
	
	if len(config["BASEFILE"]["destination"]) > 3:
		path_destination = config["BASEFILE"]["destination"].replace("custom",os.getlogin())
		if not os.path.exists(): os.makedirs(path_destination)

		abs_path_destination = os.path.join(path_destination,config["BASEFILE"]["destination_filename"])
		postgres.get_data(connection=conn, sql_query=f"query/{config["QUERY"]["name_file"]}",abs_path_destination=abs_path_destination,data_feather=config["TABLE"]["data_feather"],debug=debug)
		postgres.debug_code(debug,"Downloaded data",abs_path_destination)

	if '1' in config["TABLE"]["insert_data"]:
		postgres.insert_into(
			connection=conn
			,table=config["TABLE"]["name"]
			,dataframe=df
		)
		postgres.debug_code(debug,"Inserted data")
	
	if not (usp_name == " " or usp_name == ""):
		postgres.exec_procedure(
			connection=conn
			,database=database
			,name_procedure=config["PROCEDURE"]["name"]
			,date_initial= config["PROCEDURE"]["start_date"]
			,date_final=config["PROCEDURE"]["end_date"]
			,debug=debug
		)
		postgres.debug_code(debug,"Executed USP")

	conn.close()


if __name__ == "__main__":
	main()