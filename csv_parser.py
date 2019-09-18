import sqlite3
import pandas as pd
import os
from pathlib import Path 
import collections
import random
import sqlalchemy
from argparse import ArgumentParser
import logging
from datetime import datetime
from argparse import ArgumentParser


if __name__ == "__main__":  

    time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    logging.basicConfig(filename='csv_to_sql_parser_log_'+time+'.log',level=logging.DEBUG)

    parser = ArgumentParser()
    parser.add_argument("-f", "--file", dest="path_to_file",help="write the path of the file")
    parser.add_argument("-n", "--cname", dest="cont_name",help="write the conatiner name")
    parser.add_argument("-pw", "--rootpassword", dest="root_password",help="write the root pass defoned wnhe the container was started")
    parser.add_argument("-pr", "--containerport", dest="port",type=int,help="write the PORT number associated with the container defoned wnhe the container was started")
    args = parser.parse_args()

    if args.path_to_file:

        if not args.cont_name:
            args.cont_name = "mysql_temp"
        if not args.root_password:
            args.root_password = "root"
        if not args.port:
            args.port = "3306"

        config = {
            'host': 'localhost',
            'port': args.port,
            'user': "root" ,
            'password': args.root_password,
            'database': 'temp'
        }

        db_user = config.get('user')
        db_pwd = config.get('password')
        db_host = config.get('host')
        db_port = config.get('port')
        db_name = config.get('database')


        connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}'
       
        engine = sqlalchemy.create_engine(connection_str)
        connection = engine.connect()
        connection.execute("DROP DATABASE IF EXISTS temp;")
        connection.execute("CREATE DATABASE temp;")
        connection.close()


        connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'
        engine = sqlalchemy.create_engine(connection_str)
        connection = engine.connect()

        folder_path = Path(args.path_to_file)
       
        files = os.listdir(folder_path)

        list_of_files_to_process = []

        for f in files:
            if f.lower().endswith('.csv'):
                list_of_files_to_process.append(f)


        files_set = set(list_of_files_to_process)

        print(len(list_of_files_to_process)," unique files out of",len(files_set),"to process....", sep=" ")

        i=1
        for f in list_of_files_to_process:

            path_file = folder_path / f
            csv_file = pd.read_csv(path_file)


            csv_file.columns = map(str.lower, csv_file.columns)
            csv_file.columns = csv_file.columns.str.strip()

            dup_col_msg = False
            
            for col in csv_file.columns:
                if list(csv_file.columns).count(col) > 1:
                    index = list(csv_file.columns).index(col)
                    csv_file.columns.values[index] = col+"_duplicated_column_"+str(random.randint(1,50))
                    dup_col_msg = True

            csv_file.to_sql(name=path_file.stem, con=connection, index=False)

            print(">>>",i,"of",len(list_of_files_to_process), sep=" ")
           
            msg = ""
            if dup_col_msg: 
                msg = " - duplicated columm"
            logging.info(">>> " + str(i)+" of "+ str(len(list_of_files_to_process))+ msg)

            i += 1
            

        print("dumping tables to SQL")
        logging.info("dumping tables to SQL")
            
        myCmd = 'docker exec -it '+args.cont_name+' mysqldump temp > output_'+time+'.sql -uroot -proot'
        os.system(myCmd)

        with open('output_'+time+'.sql', 'r+') as f: #open in read / write mode
            
            f.readline() #read the first line and throw it out
            data = f.read() #read the rest
            f.seek(0) #set the cursor to the top of the file
            f.write(data) #write the data back
            f.truncate() #set the file size to the current size


        logging.info(".sql file created")


    else:
        print("No path for a file was provided")
        exit



