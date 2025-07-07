import mysql.connector
import md_logfile as lf
from datetime import datetime
from time import sleep
from os import path as ospath
from os import getcwd

def debug_code(debug,message,var=None):
	if debug == 1: print(f"{message}: {var};\r\n")

def connection(auth_plugin,system_type=None,database=None,user=None,password=None,host=None,debug=None):
	debug_code(debug,"Credentials",f"database={database},user={user},password={password},host={host}")

	"""if get_data == 1:
		dict_cotacao = {"database": "asas","user": "","password": "","auth_plugin": auth_plugin,"host": "asas.cluster-ro-c7k847dd6529.us-east-1.rds.amazonaws.com"}
		dict_gestao = {"database": "bia","user": "" ,"password": "","auth_plugin": auth_plugin,"host": "dbgestao.cn8n3g0jedzx.us-east-1.rds.amazonaws.com"}

		if system_type == 1: dict_config = dict_cotacao
		else: dict_config = dict_gestao
	else:"""

	dict_config = {"database": database,"user": user ,"password": password,"auth_plugin": auth_plugin,"host": host}
	
	try: 
		connection = mysql.connector.connect(**dict_config) ## **to unpack dict to fill all parameteres of connect method
		return(connection)

	except mysql.connector.Error as error:
		debug_code(debug,(error,"Failed, can't connect to database :["))
		lf.log_file("log_error",f"Step: 'DB Connection'", datetime.today())


def get_data(connection,sql_query,debug):
		
	with open(ospath.join(getcwd(),sql_query+".sql"), 'r', encoding='utf-8') as file: sql_query = file.read()

	try: 
		cursor = connection.cursor()    
		for result in cursor.execute(sql_query, multi=True):
			if result.with_rows: result_set = result.fetchall()

		cursor.close()
		if result_set is None or result_set == []: lf.log_file("log_error",f"Step: 'Data Extract'", datetime.today(), message=f"Empty result set!! \n\n Number of rows affected [{result.rowcount}] by statement:\n '{result.statement}'")
	except mysql.connector.Error as error:
		debug_code(debug,(error,"Failed, can't extract data :( "))
		lf.log_file("log_error",f"Step: 'Data Extract'", datetime.today())

	return(result_set)

def insert_into(connection,database,table,dataframe,column_size):
		cursor = connection.cursor()
		try:
			data_to_insert = list(dataframe.itertuples(index=False, name=None))
			print(f"column_size: {column_size}")
			cursor.execute(f"TRUNCATE TABLE {database}.{table}")
			query = f"INSERT INTO {database}.{table} VALUES (%s{', %s'*(column_size-1)})"
			
			cursor.executemany(query, data_to_insert)
			
			connection.commit()
		except Exception as error:
			print(f"[Error]: {error}")

		sleep(15)
		#cursor.close()	

def exec_procedure(connection,database,name_procedure,date_initial,date_final,debug=None):
	cursor = connection.cursor()
	if date_initial == " " or date_initial == "": date_initial = 'NULL'
	if date_initial == " " or date_final == "": date_final = 'NULL'
	
	debug_code(debug,'date_initial', date_initial)
	debug_code(debug,'date_final', date_final)

	try:
		sql_query = f"CALL {database}.{name_procedure}({date_initial}, {date_final});"
		print(sql_query)
		cursor.execute(sql_query)
		connection.commit()
	except Exception as error:
		print(f"[Error]: {error}")

		sleep(15)
	#cursor.close()