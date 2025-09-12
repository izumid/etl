import md_sqlutil.sql_server as sql_server
import configparser
import os
import pandas as pd
from datetime import datetime
import re
import json
import md_genconfig as gc
import md_logfile as lf
import sys

def main():
	try:
		config = configparser.RawConfigParser()
		path_config = os.path.join(os.getcwd(),"config")
		abs_path_config = os.path.join(path_config,"config.ini")
		config.read(abs_path_config)
		debug = config["STATUS"]["debug"]
		#column_to_date = config["DATASET"]["column_to_date"]
		usp_name = config["PROCEDURE"]["name"]
		with open(os.path.join(os.getcwd(),r"config/config.json")) as jsf: config_json = json.load(jsf)
		export_config_name= config["CONFIG"]["name"]
		
		conn = sql_server.connection(db_config=config_json,debug=debug)
		
		sql_server.debug_code(debug,"connection variable",conn)
		
		if len(config["BASEFILE"]["destination"]) > 3:
			path_destination = config["BASEFILE"]["destination"].replace("custom",os.getlogin())
			if not os.path.exists(path_destination): os.makedirs(path_destination)

			abs_path_destination = os.path.join(path_destination,config["BASEFILE"]["destination_filename"])
			sql_server.get_data(connection=conn, sql_query=f"query/{config["QUERY"]["name_file"]}",abs_path_destination=abs_path_destination,data_feather=config["BASEFILE"]["name"],debug=debug)
			sql_server.debug_code(debug,"Downloaded data",abs_path_destination)
		
		if '1' in config["TABLE"]["insert_data"]:
			sql_server.insert_into(
				connection=conn
				,table=config["TABLE"]["name"]
				,path_origin = config["BASEFILE"]["path_in"]
				,insert_by_row=config["TABLE"]["insert_by_row"]
				,truncate=config["TABLE"]["truncate"]
				,debug=debug
				,control_data_field=(config["DATASET"]["control_data_field"]).lower()
				,column_trim=config["DATASET"]["column_trim"]
				,column_to_date=config["DATASET"]["column_to_date"]
			)
			sql_server.debug_code(debug,"Inserted data")
		
		# if not (usp_name == " " or usp_name == ""):
		# 	sql_server.exec_procedure(
		# 		connection=conn
		# 		,name_procedure=config["PROCEDURE"]["name"]
		# 		,date_initial= config["PROCEDURE"]["start_date"]
		# 		,date_final=config["PROCEDURE"]["end_date"]
		# 		,debug=debug
		# 	)
		# 	sql_server.debug_code(debug,"Executed USP")

		conn.close()
		
		if export_config_name != "" and export_config_name != " ":
			gc.generate_config_file(path_file_in=path_config,path_file_out=path_config,name_config=os.path.basename(abs_path_config),name_new_config=export_config_name,summary=config["CONFIG"]["summary"],commentary=config["CONFIG"]["commentary"])
			sys.stdout.write(1)
	except Exception as error:
		lf.log_file(filename="log_file",header_message="In Main Error",message=error)
		sys.stdout.write(0)

if __name__ == "__main__":
	main()