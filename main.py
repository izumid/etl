import md_mysqlutil
import md_logfile
import configparser
import os
import pandas as pd
from datetime import datetime
import re
import numpy as np

def data_read(path_relative,filename,extension,debug=None):
	filename = filename+"."+extension
	na_values = ['', ' ', 'NA', 'N/A', 'na', 'n/a', 'null', 'NULL', 'none', 'None', 'NaN', 'nan', 'NAN', 'NaT', 'nat']

	path_absolute = os.path.join(path_relative.replace("custom",os.getlogin()), filename)
	dataframe = pd.read_csv(path_absolute,dtype=str,sep=";",na_values=na_values,keep_default_na=True,index_col=None,parse_dates=None)
	#dataframe = dataframe.astype(str)
	#dataframe = dataframe.replace({NaN: None})
	md_mysqlutil.debug_code(debug,"dataframe information",dataframe.info())
	return(dataframe)

def try_cast_date(x):
	after_trim_pattern = r"\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b"
	br_pattern = r"\b\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}\b"

	# not bool(re.search(br_pattern, x)) == bool(re.search(br_pattern, x)) == False
	if isinstance(x, str) and not bool(re.search(br_pattern, x)) and not bool(re.search(after_trim_pattern, x)):
			#print(type(x),x)
			x = re.sub(r'[^A-Za-z0-9/]+', '', x)
			try: x = datetime.strptime(x, "%d/%m/%Y")
			except: x = None
	#else: print(type(x),x,"---------------------------------------------------")

	return x

def main():
	config = configparser.RawConfigParser()
	config.read(os.path.join(os.getcwd(),r"config/config.ini"))
	debug = int(config["STATUS"]["debug"])
	auth_plugin = config["DATABASE"]["auth_plugin"]
	database =config["DATABASE"]["database"]
	column_name_modify = config["DATASET"]["column_name_modify"]
	column_to_date = config["DATASET"]["column_to_date"]
	usp_name = config["PROCEDURE"]["name"]

	df = data_read(
		path_relative=config["BASEFILE"]["path_in"]
		,filename=config["BASEFILE"]["list_name_file"]
		,extension=config["BASEFILE"]["extension"]
		,debug=debug
	)
	
	if column_name_modify:
		if '1' in config["DATASET"]["column_trim"]:
			md_mysqlutil.debug_code(debug,"Trim and ajust to data")
			df[column_name_modify] = df[column_name_modify].apply(lambda x: try_cast_date(x))

		# df.to_csv(os.path.join(os.getcwd(),"hist_issuance.csv"),sep=";",index=False)

		if '1' in column_to_date:
			md_mysqlutil.debug_code(debug,"Cast to date")
			df[column_name_modify] = pd.to_datetime(df[column_name_modify], errors='coerce')
			# Must be convert explicity (cast) to string.
			df[column_name_modify] = df[column_name_modify].dt.strftime("%Y-%m-%d")
			#df[column_name_modify] = pd.to_datetime(df[column_name_modify])
			"""
			#Lines below doesn't work, because object is a generic type, not specifically string.
			df[column_name_modify] = df[column_name_modify].astype("object")	
			df[column_name_modify] = df[column_name_modify].where(df[column_name_modify].notna(), None)
			"""
	df = df.where(df.notnull(), None)
	print(df.info())
	
	#df.to_csv(os.path.join(os.getcwd(),"hist_issuanceII.csv"),sep=";",index=False)

	conn = md_mysqlutil.connection(
		auth_plugin=auth_plugin
		,database=database
		,user=config["DATABASE"]["user"]
		,password=config["DATABASE"]["password"]
		,host=config["DATABASE"]["host"]
		,debug=debug
	)
	
	md_mysqlutil.debug_code(debug,"connection variable",conn)
	
	if '1' in config["TABLE"]["get_data"]:
		result_set =	md_mysqlutil.get_data(
				connection=conn
				,sql_query=config["QUERY"]["name_file"]
				,debug=debug
			)

		print(result_set)

	if '1' in config["TABLE"]["insert_data"]:
		md_mysqlutil.insert_into(
			connection=conn
			,database=database
			,table=config["TABLE"]["name"]
			,dataframe=df
			,column_size=len(df.columns.to_list())
		)

		#md_mysqlutil.debug_code(debug,"Data was insert")

			
	if not (usp_name == " " or usp_name == ""):
		md_mysqlutil.exec_procedure(
			connection=conn
			,database=database
			,name_procedure=config["PROCEDURE"]["name"]
			,date_initial= config["PROCEDURE"]["start_date"]
			,date_final=config["PROCEDURE"]["end_date"]
			,debug=debug
		)
		md_mysqlutil.debug_code(debug,"Data was executed")

	conn.close()
if __name__ == "__main__":
	main()