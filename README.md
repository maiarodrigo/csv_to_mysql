### Parsing CSV files to MySql Scripts

1. Clone the repo

2. Run Mysql Container with mysql latest or the target version you are creating the data to be imported

`docker run --name=mysql_temp --env="MYSQL_ROOT_PASSWORD=root" -p 3306:3306 -d mysql:latest`
or
`docker run --name=mysql_temp --env="MYSQL_ROOT_PASSWORD=root" -p 3306:3306 -d mysql:5.7.27`


3. Install python dependencies

`pip install -r requirements.txt`


4. Run script

Script Options
-h : help 
-f : path to the folder containing the csv files
-n : Database container name
-pw : password defined for the mysql root user
-pr : port number of the running container

Obs: if docker container was initialized with the same as the above paramentes, the only necessary argument is the path to the folder 

`python csv_parser.py -f \path\to\folder or path/to/folder`

results are a log file and the sql dump formated for the mysql database

5. import into database 
- Create database first
- Make sure the database user has the necessary privilegies for the database

$ mysql -u YOUR_USER -p DATABASE_NAME < output_#######.sql












