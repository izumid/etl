import pyodbc
import os
import sys
from time import sleep
import re
from datetime import datetime
import pandas as pd
from pathlib import Path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
import md_logfile as lf


def debug_code(debug,message,var=None):
	if '1' in debug: 
		if var is None: print(f"{message};\r\n")
		else: print(f"{message}: \r\n{var};\r\n")

def data_read(path_absolute,debug):

	na_values = ['', ' ', 'NA', 'N/A', 'na', 'n/a', 'null', 'NULL', 'none', 'None', 'NaN', 'nan', 'NAN', 'NaT', 'nat']
	#path_absolute = os.path.join(path_relative.replace("custom",os.getlogin()), filename)

	if Path(os.path.basename(path_absolute)).suffix == ".feather": dataframe = pd.read_feather(path_absolute)
	else: dataframe = pd.read_csv(path_absolute,dtype=str,sep=";",na_values=na_values,keep_default_na=True,index_col=None,parse_dates=None)
	debug_code(debug,"dataframe header",dataframe.head())
	
	return(dataframe)


def try_cast_date(x):
	after_trim_pattern = r"\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b"
	brazilian_pattern = r"\b\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}\b"

	if isinstance(x, str) and not bool(re.search(brazilian_pattern, x)) and not bool(re.search(after_trim_pattern, x)):
		x = re.sub(r'[^A-Za-z0-9/]+', '', x)
		try: x = datetime.strptime(x, "%d/%m/%Y")
		except: x = None

	return x


def connection(db_config,debug=None):
	debug_code(debug,"Credentials",f"database={db_config["database"]},user={db_config["user"]},password={db_config["password"]},host={db_config["host"]}")
	driver = ['SQL Server', 'SQL Server Native Client 11.0', 'ODBC Driver 17 for SQL Server', 'ODBC Driver 18 for SQL Server']
		
	try:
		connection = pyodbc.connect(
			f"DRIVER={driver[0]};"
			f"SERVER={db_config["host"]};"
			f"DATABASE={db_config["database"]};"
			f"UID={db_config["user"]};"
			f"PWD={db_config["password"]};"
			f"TrustServerCertificate=no;"
		)

		return connection
	except pyodbc.Error as error:
		sqlstate = error.args[0]
		lf.log_file(filename="log_file",header_message="Database Connection",message=error)


def get_data(connection,sql_query,abs_path_destination,data_feather,debug):
	"""	
		with open(ospath.join(getcwd(),sql_query+".sql"), 'r', encoding='utf-8') as file: sql_query = file.read()

		try: 
			df = pd.read_sql(sql_query, connection)
			if '.feather' in data_feather: 
				abs_path_destination = abs_path_destination + ".feather"
				df.to_feather(abs_path_destination)
			else: 
				abs_path_destination = abs_path_destination + ".csv"
				df.to_csv(abs_path_destination,sep=';',encoding="utf-8")
		except Exception as error:
			debug_code(debug,(error,"Failed, can't extract data :( "))
			lf.log_file(filename="log_file",header_message="Select Data",message=error)
	"""
	pass


def insert_into(connection,table,path_origin,insert_by_row,truncate,debug,control_data_field,column_trim,column_to_date):

	cursor = connection.cursor()
	for basefile in os.listdir(path_origin):
		path_absolute = os.path.join(path_origin,basefile)
		print("Aaaaaaaaaaaa", path_absolute)
		if os.path.isfile(path_absolute):
			df=data_read(path_absolute=path_absolute,debug=debug)
			debug_code(debug,"dataframe header",df.head())

			if control_data_field:
				if '1' in column_trim:
					debug_code(debug,"Trim and ajust to data")
					df[control_data_field] = df[control_data_field].apply(lambda x: try_cast_date(x))

				if '1' in column_to_date:
					debug_code(debug,"Cast to date")
					df[control_data_field] = pd.to_datetime(df[control_data_field], errors='coerce')
					df[control_data_field] = df[control_data_field].dt.strftime("%Y-%m-%d")

			df = df.where(df.notnull(), None)
			column_size = len(df.columns.tolist())
			data_to_insert = list(df.itertuples(index=False, name=None))
			query = f"INSERT INTO {table} VALUES (?{', ?'*(column_size-1)})"
			
			debug_code(debug,"Dataframe column size", column_size)
			debug_code(debug,"Dataframe information", df.info())
			
			if '1' in truncate: cursor.execute(f"TRUNCATE TABLE {table}")

			if '1' in insert_by_row:
				for i, row in enumerate(data_to_insert):
					try: cursor.execute(query, row)
					except pyodbc.Error as error: print(f"Insert Data Error: Row {i + 1}: {row}\r\nError:{str(error)}\r\n\r\n")
					else: cursor.commit()
			else: 
				try:
					cursor.executemany(query, data_to_insert)
					connection.commit()
				except pyodbc.Error as error:
					lf.log_file(filename="log_file",header_message="Insert Data",message=error)
				else: cursor.commit()

			sleep(15)


def exec_procedure(connection,name_procedure,date_initial,date_final,debug=None):
	"""
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
	"""
	pass