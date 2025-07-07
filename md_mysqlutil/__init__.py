import mysql.connector
import md_logfile as lf
from datetime import datetime
from time import sleep
from os import path as ospath
from os import getcwd
from sqlalchemy import create_engine, text
import urllib.parse
import sqlalchemy.dialects.postgresql
import psycopg2
from sqlalchemy.orm import sessionmaker
import numpy as np


# https://www.geeksforgeeks.org/connecting-postgresql-with-sqlalchemy-in-python/
#https://www.tutorialspoint.com/connecting-postgresql-with-sqlalchemy-in-python

def debug_code(debug,message,var=None):
	if debug == 1: print(f"{message}: {var};\r\n")

def connection(auth_plugin,system_type=None,database=None,user=None,password=None,host=None,debug=None):
	debug_code(debug,"Credentials",f"database={database},user={user},password={password},host={host}")
	engine = create_engine(f"postgresql://{user}:{urllib.parse.quote_plus(password)}@{host}:5432/{database}")
	try: 
		connection = engine.connect()
		# connection = mysql.connector.connect(**dict_config) ## **to unpack dict to fill all parameteres of connect method
		
		#Session = sessionmaker(bind=engine)
		#session = Session()
		#return session
		return connection

	except mysql.connector.Error as error:
		debug_code(debug,(error,"Failed, can't connect to database :["))
		lf.log_file("log_error",f"Step: 'DB Connection'", datetime.today())


def get_data(connection,sql_query,debug):
		
	with open(ospath.join(getcwd(),sql_query+".sql"), 'r', encoding='utf-8') as file: sql_query = file.read()

	try: 
		cursor = connection.cursor()    
		for result in cursor.execute(sql_query, multi=True):
			if result.with_rows: result_set = result.fetchall()

		#cursor.close()
		if result_set is None or result_set == []: lf.log_file("log_error",f"Step: 'Data Extract'", datetime.today(), message=f"Empty result set!! \n\n Number of rows affected [{result.rowcount}] by statement:\n '{result.statement}'")
	except mysql.connector.Error as error:
		debug_code(debug,(error,"Failed, can't extract data :( "))
		lf.log_file("log_error",f"Step: 'Data Extract'", datetime.today())

	return(result_set)



def insert_into(connection,database,table,dataframe,column_size,match_db_columns=False):
		try:
			sql_query = f"TRUNCATE TABLE {table}"
			connection.execute(text(sql_query))
			print(f"Truncate table {table}")

			if not match_db_columns:
				columns_list = dataframe.columns.tolist()
				columns_str = ", ".join(columns_list)
				#sql_query = text(f"INSERT INTO {table} ({columns_str}) VALUES ({', '.join([':' + col for col in columns_list])})")
				sql_query = text(f"INSERT INTO {table} VALUES ({', '.join([':' + col for col in columns_list])})")
				data_list = dataframe.to_dict(orient="records")  
				connection.execute(sql_query, data_list)
				
			else: dataframe.to_sql(name=table, con=connection, if_exists="append", index=False, method="multi")	
			
			connection.commit()
			print("Inserted Data")
		except (Exception, psycopg2.DatabaseError) as error:
			print(f"[Error] Insert data: {error}")

		sleep(5)
		#cursor.close()	

def exec_procedure(connection,database,name_procedure,date_initial,date_final,debug=None):
	if date_initial == " " or date_initial == "": date_initial = 'NULL'
	if date_initial == " " or date_final == "": date_final = 'NULL'
	
	debug_code(debug,'date_initial', date_initial)
	debug_code(debug,'date_final', date_final)

	try:
		sql_query = f"CALL {name_procedure}({date_initial}, {date_final});"
		print(sql_query)
		connection.execute(text(sql_query))
		connection.commit()
	except Exception as error:
		print(f"[Error]: {error}")

		sleep(15)
	#cursor.close()