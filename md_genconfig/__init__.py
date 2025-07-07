import os
import configparser

def generate_config_file(path_file_in,path_file_out,name_config, name_new_config,header=""):

    ls=[]
    ext = ".ini"

    if header != "":
        with open(os.path.join(path_file_in, name_config+ext), 'r', encoding='utf-8') as file:
            matrix = file.read().split("\n")
            for row in matrix:
                if row.endswith(';'): ls.append(str(row))
                if not row.startswith(';'): ls.append(str(row))

        with open(os.path.join(path_file_out, name_new_config+ext), 'w', encoding='utf-8') as file:
            for row in ls:
                file.write(f"{str(row)} \n")
    else:
        config = configparser.RawConfigParser()
        config.read(os.path.join(path_file_in, name_config+ext), encoding="utf-8")

        with open(os.path.join(path_file_out, name_new_config+ext), 'w', encoding='utf-8') as file: 
            for section in config.sections():
                file.write(f"[{section}]\n")
                print(f"[{section}]\n")
                for option in config[section]:
                    file.write(f"{option} = {config[section][option]}\n")
                    print(f"{option} = {config[section][option]}\n")
                file.write("\n")