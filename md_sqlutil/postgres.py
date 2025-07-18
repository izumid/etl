
from time import sleep
from os import path as ospath
from os import getcwd
from sqlalchemy import create_engine, text
import urllib.parse
import psycopg2
import pandas as pd
import os
import sys


current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
import md_logfile as lf


def debug_code(debug,message,var=None):
	if '1' in debug: 
		if var is None: print(f"{message};\r\n")
		else: print(f"{message}: \r\n{var};\r\n")


def connection(db_config,debug=None):
	debug_code(debug,"Credentials",f"database={db_config["database"]},user={db_config["user"]},password={db_config["password"]},host={db_config["host"]}")
	engine = create_engine(f"postgresql://{db_config["user"]}:{urllib.parse.quote_plus(db_config["password"])}@{db_config["host"]}:5432/{db_config["database"]}")
	try: 
		connection = engine.connect()
		return connection
	except (Exception, psycopg2.DatabaseError) as error:
		debug_code(debug,(error,"Failed, can't connect to database :["))
		lf.log_file(filename="log_file",header_message="Database Connection",message=error)


def get_data(connection,sql_query,abs_path_destination,data_feather,debug):
		
	with open(ospath.join(getcwd(),sql_query+".sql"), 'r', encoding='utf-8') as file: sql_query = file.read()

	try: 
		df = pd.read_sql(sql_query, connection)
		if '1' in data_feather: 
			abs_path_destination = abs_path_destination + ".feather"
			df.to_feather(abs_path_destination)
		else: 
			abs_path_destination = abs_path_destination + ".csv"
			df.to_csv(abs_path_destination,sep=';',encoding="utf-8")
	except Exception as error:
		debug_code(debug,(error,"Failed, can't extract data :( "))
		lf.log_file(filename="log_file",header_message="Select Data",message=error)


def insert_into(connection,table,dataframe,match_db_columns=False):
		try:
			sql_query = f"TRUNCATE TABLE {table}"
			connection.execute(text(sql_query))

			if not match_db_columns:
				columns_list = dataframe.columns.tolist()
				sql_query = text(f"INSERT INTO {table} VALUES ({', '.join([':' + col for col in columns_list])})")
				data_list = dataframe.to_dict(orient="records")  
				connection.execute(sql_query, data_list)
				#print("sql_query", sql_query)
				
			else: dataframe.to_sql(name=table, con=connection, if_exists="append", index=False, method="multi")	
			
			connection.commit()
			sleep(5)
		except (Exception, psycopg2.DatabaseError) as error:
			lf.log_file(filename="log_file",header_message="Insert Data",message=error)


def exec_procedure(connection,name_procedure,date_initial,date_final,debug=None):
	if date_initial == " " or date_initial == "": date_initial = 'NULL'
	if date_initial == " " or date_final == "": date_final = 'NULL'
	
	debug_code(debug,'date_initial', date_initial)
	debug_code(debug,'date_final', date_final)

	try:
		sql_query = f"CALL {name_procedure}({date_initial}, {date_final});"
		connection.execute(text(sql_query))
		connection.commit()
		sleep(15)
	except (Exception, psycopg2.DatabaseError) as error:
		lf.log_file(filename="log_file",header_message="Procedure's Execution",message=error)
