import pandas as pd
import os
from pathlib import Path 
import collections
import random
import sqlalchemy
from argparse import ArgumentParser
import logging
from datetime import datetime
import math


if __name__ == "__main__":  

    time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    

    parser = ArgumentParser()
    parser.add_argument("-f", "--file", dest="path_to_file",help="write the path of the file")
    parser.add_argument("-n", "--cname", dest="cont_name",help="write the conatiner name")
    parser.add_argument("-pw", "--rootpassword", dest="root_password",help="write the root pass defoned wnhe the container was started")
    parser.add_argument("-pr", "--containerport", dest="port",type=int,help="write the PORT number associated with the container defoned wnhe the container was started")
    parser.add_argument("-tp", "--tableprefix", dest="table_prefix",help="optional - table prefixes")

    args = parser.parse_args()

    if args.path_to_file:

        if not args.cont_name:
            args.cont_name = "mysql_temp"
        if not args.root_password:
            args.root_password = "root"
        if not args.port:
            args.port = "3306"
        if not args.table_prefix:
            args.table_prefix = ""
        else:
            args.table_prefix = args.table_prefix + "_"

        config = {
            'host': 'localhost',
            'port': args.port,
            'user': "root" ,
            'password': args.root_password,
            'database': 'temp',
            'database_insert': 'temp_insert',
            "table_prefix" : args.table_prefix
        }

        db_user = config.get('user')
        db_pwd = config.get('password')
        db_host = config.get('host')
        db_port = config.get('port')
        db_name = config.get('database')
        db_name_insert = config.get('database_insert')
        db_tb_prefix = config.get('table_prefix')

        logging.basicConfig(filename=db_tb_prefix+'csv_to_sql_parser_log_'+time+'.log',level=logging.DEBUG)
        connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}'
       
        engine = sqlalchemy.create_engine(connection_str)
        connection = engine.connect()
        connection.execute("DROP DATABASE IF EXISTS temp;")
        connection.execute("CREATE DATABASE temp;")
        connection.close()

        connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'

        engine = sqlalchemy.create_engine(connection_str)
        connection = engine.connect()

        #####################################


        connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}'
       
        engine_insert = sqlalchemy.create_engine(connection_str)
        connection_insert = engine_insert.connect()
        connection_insert.execute("DROP DATABASE IF EXISTS temp_insert;")
        connection_insert.execute("CREATE DATABASE temp_insert;")
        connection_insert.close()

        connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name_insert}'

        engine_insert = sqlalchemy.create_engine(connection_str)
        connection_insert = engine_insert.connect()



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
            
            print(">>>",i,"of",len(list_of_files_to_process), sep=" ")
            
            path_file = folder_path / f
            csv_file = pd.read_csv(path_file)


            csv_file.columns = map(str.lower, csv_file.columns)
            csv_file.columns = csv_file.columns.str.strip()

            dup_col_msg = ""
            dataType_col_msg = ""
            
            for col in csv_file.columns:
                if list(csv_file.columns).count(col) > 1:
                    index = list(csv_file.columns).index(col)
                    csv_file.columns.values[index] = col+"_duplicated_column_"+str(random.randint(1,50))
                    dup_col_msg = " - duplicated columm - "+col
                if csv_file[col].dtype == "uint64":
                    csv_file = csv_file.astype({col:str})
                    dataType_col_msg = " - Change of data type - "+col
            
            logging.info(">>> " + str(i)+" of "+ str(len(list_of_files_to_process))+ dup_col_msg + dataType_col_msg)

            if csv_file.shape[0] < 1000:
                csv_file.to_sql(name=db_tb_prefix+path_file.stem, con=connection, index=False)
                logging.info(">>>>>>>> " + str(i) + " table - "+ str(db_tb_prefix+path_file.stem) + " - Create Table with " + str(csv_file.shape[0]) )
            else:
                #print("shape inicial", csv_file.shape, sep=" ")
                n_out = math.ceil((csv_file.shape[0]*10)/100)
                #print("N", n_out, sep=" ")
                insert_data = csv_file.iloc[- n_out:]
                #print("shape insert_data", insert_data.shape, sep=" ")
                load_data = csv_file.drop(csv_file.tail(n_out).index,inplace=False)
                #print("shape load_data", load_data.shape, sep=" ")
                load_data.to_sql(name=db_tb_prefix+path_file.stem, con=connection, index=False)
                insert_data.to_sql(name=db_tb_prefix+path_file.stem, con=connection_insert, index=False)
                load_size = str(load_data.shape[0])
                insert_size = str(insert_data.shape[0])
                logging.info(">>>>>>>> " + str(i) + " table - "+ str(db_tb_prefix+path_file.stem) + " - original records: "+ str(csv_file.shape[0]) + " - Create Table with " + load_size + " records - Insert with " + insert_size + " records" )
            i += 1
    

        print("dumping tables to SQL")
        logging.info("dumping tables to SQL")
            
        myCmd = 'docker exec -it '+args.cont_name+' mysqldump temp > '+db_tb_prefix+'create_'+time+'.sql -uroot -proot'
        os.system(myCmd)

        myCmd = 'docker exec -it '+args.cont_name+' mysqldump --no-create-info temp_insert > '+db_tb_prefix+'insert_'+time+'.sql -uroot -proot'
        os.system(myCmd)

        with open(db_tb_prefix+'create_'+time+'.sql', 'r+') as f: #open in read / write mode
            
            f.readline() #read the first line and throw it out
            data = f.read() #read the rest
            f.seek(0) #set the cursor to the top of the file
            f.write(data) #write the data back
            f.truncate() #set the file size to the current size

        with open(db_tb_prefix+'insert_'+time+'.sql', 'r+') as f: #open in read / write mode
            
            f.readline() #read the first line and throw it out
            data = f.read() #read the rest
            f.seek(0) #set the cursor to the top of the file
            f.write(data) #write the data back
            f.truncate() #set the file size to the current size


        logging.info(".sql file created")


    else:
        print("No path for a file was provided")
        exit



