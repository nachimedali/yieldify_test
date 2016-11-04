## Test 1 repository

#### Overview
I have used Python 2.7 and Spark (Pyspark) to parallelize reading files and generating required results.
**Kindly note that you need Spark and Hadoop to be installed, also Python path need to be linked to pyspark**


Librairies used are available in file req.txt:
1. boto : To create connection to S3 bucket and be able to get data or to upload it
2. pygeoip : With collecting data from ./data/GeoLiteCity.dat, it permits to get location data from ip adress
3. tinys3 : I was not able to upload data using boto due to some errors from my computer, so I had, to upload data, to use another lib which is tinys3 
4. user_agents : a lib that permits to parse user agent data and return data required.

#### The structure
-Main Folder /
-- script.py : that runs the script
-- data /
--- log.json : a json file containing "processed_files" for files processed successfully, "not_processed_files" for failed processed files and "final_date" that contains the last datestamp of files downloaded
--- GeoLiteCity.dat : Data required to use with lib pygeoip

#### The script
1. **_max-date()_** : From folders available in data/ in s3, it allows to extract the last datestamp of files uploaded. This method is used to generate the last datestamp of data uploaded to check if new files where uploaded after our final process.
2. **_check-new-files()_** : after generating our max datestamp, it tests if files uploaded in the max folder have been processed or not. In other words, it tests if new files were uploaded in max folder py checking if they are present in processed files list in our log file
3. **_download-file_** : it permits to download a file from s3 bucket repository and store it in local
4. **_decompress-file_** : After downloading a file, this method allow to unzip it
5. **_zip-file_** : Allowing to zip generated data
6. **_validate-row_** : using regex, it permits to filter rows that matches our structure
7. **_timstp_** : convert date rows to datestamp
8. **_getlocation_** : convert ip adress to longitude, latitude, city and country
9. **_parse-user-agent_** : permits to parse user agent row and extract required data
10. **_genobject_** : permits to generate an object for each validated row
11. **_generate_files_** : Uses **Spark** context to **map reduce** a file, get required data and generate result file.
This picture would simplify the process:
![alt text][mapreduce]
[mapreduce]: ./images/mapreduce.png "mapreduce"
12. **_upload-files_** : permits to upload resulted file to s3 after zipping
13. **_definenewfiles_** : is the main of our project, it permits to verify if new updates are, and to call different task

#### Running 
To run the script, just install libraries with **_"sudo pip install -r req.txt"_** and run it with **_"python script.py"_**

### Task_2 : **Building an API**

I have built a simple API with **tornado** lib and **pyyaml** lib to parse json object. It uses also pyspark. That means, to run the script, you need Hadoop and spark installed and python path linked to python Spark.

**Also, Kindly note** that, as I would upload data in github, I can not add data in data repository for task 2, it uses data generated from task1 and put together without passing to folders. I mean, all  files put together in /data folder.

#### The structure
- Main folder/
-- task2.py : script generating the requested api for the 4 first calls. 
-- data/
-- < Put here all files together from s3 bucket without any repository >

#### How it works
After launching the script, open a web navigator and go to :
localhost:8888/< api here>/?start_date=< start_date >&end_date=< end_date >
Not passing start_date or end date is possible, and it would take min/max values of datastamp
