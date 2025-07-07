import os
import traceback


def write_info(file, header, message):
	file.write(header)
	if message is None: traceback.print_exc(file=file)
	else: file.write(f"{message}\n\n")
	file.write(f"{"="*50}\n\n")
	file.write("\n\n")


def log_file(filename,header_message,time,message=None):
	filename = filename + ".txt"
	header=f"===== {header_message}: {time};  =====\n\n"
    
	if not os.path.exists(os.path.join(os.getcwd(),filename)): open(filename, "x")
	
	if os.stat(filename).st_size < 3000:
		with open(filename, 'a+',encoding="utf-8") as f:
			write_info(file=f,header=header,message=message)
	else:
		with open(filename, 'w+',encoding="utf-8") as f:
			write_info(file=f,header=header,message=message)


def log_time(on,message,time):
	if on == 1: print(f"[Time] - {message} ({time})")