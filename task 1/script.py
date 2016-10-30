import os
import glob
import gzip
import re
import pygeoip
import json
import csv
import time
from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession

from user_agents import parse
import tinys3
from boto.s3.connection import S3Connection
import boto

AWS_KEY = 'AWS-KEY'
AWS_SECRET = 'AWS-Secret'
aws_connection = S3Connection(AWS_KEY, AWS_SECRET)
bucket = aws_connection.get_bucket('yi-engineering-recruitment')

## 
# The idea is to generate a list of newer files, and call download_file function to downloaditems in this list

#Returning last date when data uploaded
def max_date():
	tmstp = 0
	pattern = re.compile(r"data/[0-9]*/[0-9]*/[0-9]*/")
	bucket_list = bucket.list(prefix="data/")
	for item in bucket_list:
		if pattern.match(item.name) and '.gz' not in item.name:
			nwr = item.name.split('/')
			ti = nwr[1]+'-'+nwr[2]+'-'+nwr[3]
			timestamp = time.mktime(datetime.strptime(ti, "%Y-%m-%d").timetuple())
			if timestamp > tmstp:
				tmstp = timestamp
	return tmstp

# Checking for new files added betweed final date stored dt and max_date new_dt
def check_new_files(new_dt, dt, pf):
	new_date = datetime.fromtimestamp(new_dt)
	# print '%02d,%02d'%(last_date.day,last_date.day)
	if new_dt > dt:
		#Getting all files in data bucket
		bucket_list = bucket.list(prefix="data/")
		bucket_files = [x.name for x in bucket_list if '.gz' in x.name]
		##filtring to get only files that are updated after dt
		new_files = [x for x in bucket_files if dt <= time.mktime(datetime.strptime(x.split('/')[-4]+"-"+x.split('/')[-3]+"-"+x.split('/')[-2], "%Y-%m-%d").timetuple()) <= new_dt]
		### test if files are in processed list to
		new_files = [x for x in new_files if x.split('/')[-1] not in pf]
	elif new_dt == dt:
		##Get only files in latest repository to check if new files have been uploaded
		bucket_list = bucket.list(prefix="data/%s/%02d/%02d/" %(new_date.year,new_date.month,new_date.day))
		bucket_files = [x.name for x in bucket_list if '.gz' in x.name]
		new_files = [x for x in bucket_files if x.split('/')[-1].replace('.gz','') not in pf]
	return new_files

#Downloading files
def download_file(file):
	LOCAL_PATH = os.path.dirname(os.path.realpath(__file__)) + '/data/'
	bucket_list = bucket.list(prefix=file)
	for item in bucket_list:
		keyString = str(item.key)
		d = LOCAL_PATH + keyString
		try:
	  		item.get_contents_to_filename(d)
		except OSError:
			direct = "/".join(d.split("/")[:-1])
			if not os.path.exists(direct):
				os.makedirs(direct)
				item.get_contents_to_filename(d)

# Decompressing files
def decompress_file(fil):
	file_input = os.path.dirname(os.path.realpath(__file__)) +'/data/'+ fil
	with gzip.open(file_input, 'rb') as in_file: #Reading gzip file
		s = in_file.read()

		# remove the '.gz' from the filename
		path_to_store = file_input[:-3]+'.tsv'

		# store uncompressed file data from 's' variable
		with open(path_to_store, 'w') as out_file:
			out_file.write(s)
	in_file.close()
	out_file.close()
	os.remove(file_input)

def zip_file(fil):
	print "Generating gz file"
	outfilename = fil + '.gz'
	try:
		f_in = open(fil)
		f_out = gzip.open(outfilename, 'wb')
		f_out.writelines(f_in)
		f_out.close()
		f_in.close()
	except Exception,e:
		raise e

def validate_row(row):
	state = False
	pattern_0 = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
	pattern_1 = re.compile(r"[0-9]{2}:[0-9]{2}:[0-9]{2}")
	pattern_2 = re.compile(r"[A-Za-z0-9]{40}")
	pattern_3 = re.compile(r"(http|https)://[A-Za-z0-9]{40}/[A-Za-z0-9]{40}")
	pattern_4 = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
	try:
		pattern_0.match(row[0]).group(0)
		pattern_1.match(row[1]).group(0)
		pattern_2.match(row[2]).group(0)
		pattern_3.match(row[3]).group(0)
		user = parse(row[3])
		if row[4] != '-':
			ips = row[4].replace(' ','').split(',')
			for r in ips:
				pattern_4.match(r).group(0)
		state = True
	except:
		pass
	return state

def timstp(row):
	myDate = row[0] +' ' + row[1]
	timestamp = time.mktime(datetime.strptime(myDate, "%Y-%m-%d %H:%M:%S").timetuple())
	return timestamp

def get_location(row):
	d = {}
	gi = pygeoip.GeoIP('./data/GeoLiteCity.dat')
	if row[4] != '-':
		ips = row[4].replace(' ','').split(',')
		for ip in ips:
			try:
				location = gi.record_by_addr(ip)
				lat = location['latitude']
				lon = location['longitude']
				city = location['city']
				country = location['country_name']
				d = {"latitude": lat, "longitude" : lon, "city":city, "country":country}
			except:
				d = {"latitude": "Not available", "longitude" : "Not available", "city":"Not available", "country":"Not available"}
	else:
		d = {"latitude": "Not available", "longitude" : "Not available", "city":"Not available", "country":"Not available"}
	return d
		
	
def parse_user_agent(row):
	d = {}
	try:
		user_agent = parse(row[5])
		browserfamily = user_agent.browser.family
		osfamily = user_agent.os.family
		ismobile = user_agent.is_mobile
		d = {"mobile": ismobile, "string" : row[5], "os_family" : osfamily, "browser_family" : browserfamily}
	except:
		d = {"mobile": "Not available", "string" : row[5], "os_family" : "Not available", "browser_family" : "Not available"}
	return d

def gen_object(row):

	d = {}
	d["url"] = row[3]
	d["user_id"] = row[2]
	d["timestamp"] = timstp(row)
	d["location"] = get_location(row)
	d["user_agent"] = parse_user_agent(row)
	row[0] = d
	return row[0]

def generate_files(fil, pf, npf, sc):
	
	## Taking a file as an input, we would proceed with mapping and reducing rows in file
	
	## result is our json file resulted,
	## We start by reading file
	## We proceed by validating row
	## We generated ourdict containing our data
	## Convert to Json
	## finally generating file with adding results line by line
	try:
		result = sc.textFile(fil) \
		.map(lambda line: line.split("\t")) \
		.filter(lambda x: validate_row(x) == True)\
		.map(lambda x: gen_object(x))\
		.map(json.dumps)\
		.reduce(lambda x, y: x + "\n" + y)

		#defining Directory
		directory = './processed/nachi/{0}/{1}/{2}/'.format(fil.split('/')[-4],fil.split('/')[-3],fil.split('/')[-2])
		# creating directory if not exist
		if not os.path.exists(directory):
			os.makedirs(directory)
		#Defining file name
		filename = directory + fil.split('/')[-1].replace('.tsv','')
		with open(filename, "w") as f:
			f.write(result.encode("utf-8"))
		print "Zipping File"
		zip_file(filename)
		print "File Zipped Successfully"
		## Adding fil processed to processed file list
		pf.append(filename.split('/')[-1])
	except:
		###Case of not prcessing file, adding it to npf list
		print "Processing File {0} failed ".format(fil.split('/')[-1])
		fil_name = fil.split('/')[-1].replace('.tsv','.gz')
		if fil_name not in npf:
			npf.append(fil_name)

def upload_files(fil,conn):
	print "Start uploading {0}".format(fil)
	try:
		f = open(fil,'rb')
		conn.upload(fil.replace('./','/'),f,'yi-engineering-recruitment')
		print "File Uploaded Successfully"
	except:
		print "Uploading file failed"

			
def define_new_files():
	#Testing if log file exists to load data, otherwise, create it
	if os.path.isfile('./data/logs.json'):
		with open('./data/logs.json') as data_file:
			logs = json.load(data_file)
			pf = logs["processed_files"]
			npf = logs["not_processed_files"]
			last_date = logs["final_date"]	
	else:
		# Initializing data for log file
		# First testing if directory exists
		if not os.path.exists('./data'):
			os.makedirs('./data')
		pf = []
		npf = []
		new_date = 1000000000.0
		log = {"processed_files" : pf, "not_processed_files" : npf, "final_date":new_date}
		with open("./data/logs.json", "w") as outfile:
			json.dump(log,outfile,indent=4, sort_keys=True)  
		print 'logs file created'


	# # Get last timestamp of updating files in S3 bucket
	new_date = max_date()

	# # Generating a list containing files not processed and deployed after our last update
	# # The list would be empty if no files were uploaded after our last update
	new_files = check_new_files(new_date,last_date,pf)

	## The length of new_files list reflects if new files have been uploaded or not
	if len(new_files) == 0:
		print "no new files"
	else:
		## Proceed to downloading new files and decompressing and deleting original gz file after decompressing
		i = 1
		for new_file in new_files:
			print "Downloading file {0} / {1}...".format(i, len(new_files))
			download_file(new_file)
			print "Download finished"
			print "Start decompressing ..."
			decompress_file(new_file)
			i += 1
			print "File decompressed"
		print "Files downloaded"
		##replacing links in new_files list to work locally
		new_files = [os.path.dirname(os.path.realpath(__file__)) +'/data/'+ x.replace('.gz','.tsv') for x in new_files]
		print len(new_files)
		print "Starting generating data..."
		# Starting Spark context
		sc =SparkContext()
		for new_file in new_files:
			print "generating file {0}".format(new_file)
			generate_files(new_file,pf,npf,sc)

		##uploading generated gz files to s3 
		new_files = ['/processed/nachi/' + new_file.split('/')[-4] + '/' + new_file.split('/')[-3] + '/' + new_file.split('/')[-2] + '/'+new_file.replace('.tsv','.gz').split('/')[-1] for new_file in new_files]
		conn = tinys3.Connection(AWS_KEY,AWS_SECRET,tls=True)
		for new_file in new_files:
			upload_files(new_file, conn)
		

		log = {"processed_files" : pf, "not_processed_files" : npf, "final_date":new_date}
		with open("./data/logs.json", "w") as outfile:
			json.dump(log,outfile)


## Launch the app						
define_new_files()