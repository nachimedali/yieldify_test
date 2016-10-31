from datetime import date
import tornado.escape
import tornado.ioloop
import tornado.web

from pyspark import SparkContext
from pyspark.sql import SparkSession

import csv
import glob
import yaml
from user_agents import parse

class usersdistinct(tornado.web.RequestHandler):
    def get(self):
    	start_date = self.get_argument('start_date', '1412710000')
    	end_date = self.get_argument('end_date', '1413000000')
        dire = glob.glob("./data/*.json")
        if start_date > end_date:
            response = "Error, you have mentioned wrong dates"
        else:
            try:
                sc =SparkContext()
                # reading files simultanetly
                
                result = sc.textFile(','.join(dire)) \
                .map(lambda line: line.split("\n")) \
                .map(lambda line: line[0].replace('"','')) \
                .map(lambda line: yaml.load(line))\
                .filter(lambda x: int(start_date) <= x["timestamp"] <= int(end_date)) \
                .map(lambda x: x['user_id']).distinct().count()
                response = { 'number of distinct users': result,
                     'start_date': start_date,
                     'end_date': end_date }
                sc.stop()
            except Exception,e:
                response = "Process Failed"


        
        self.write(response)

class usersunique(tornado.web.RequestHandler):
    def get(self):
        start_date = self.get_argument('start_date', '1412719261')
        end_date = self.get_argument('end_date', '1412794861')
        dire = glob.glob("./data/*.json")

        if start_date > end_date:
            response = "Error, you have mentioned wrong dates"
        else:
            try:
                sc =SparkContext()
                # reading files simultanetly
                
                result = sc.textFile(','.join(dire)) \
                .map(lambda line: line.split("\n")) \
                .map(lambda line: line[0].replace('"','')) \
                .map(lambda line: yaml.load(line))\
                .filter(lambda x: int(start_date) <= x["timestamp"] <= int(end_date)) \
                .map(lambda x: x['user_id']) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda x,y:x+y) \
                .filter(lambda x: x[1] == 1)\
                .count()

                response = { 'number of unique users': result,
                         'start_date': start_date,
                         'end_date': end_date }
                sc.stop()
            except Exception,e:
                raise e 
        self.write(response)

class domainsdistinct(tornado.web.RequestHandler):
    def get(self):
        start_date = self.get_argument('start_date', '1412719261')
        end_date = self.get_argument('end_date', '1412794861')
        dire = glob.glob("./data/*.json")
        if start_date > end_date:
            response = "Error, you have mentioned wrong dates"
        else:
            try:
                sc =SparkContext()
                # reading files simultanetly
                
                result = sc.textFile(','.join(dire)) \
                .map(lambda line: line.split("\n")) \
                .map(lambda line: line[0].replace('"','')) \
                .map(lambda line: yaml.load(line))\
                .filter(lambda x: int(start_date) <= x["timestamp"] <= int(end_date)) \
                .map(lambda x: x["url"].split('/')[-2]).distinct().count()

                response = { 'number of distinct domains': result,
                     'start_date': start_date,
                     'end_date': end_date }
                sc.stop()
            except Exception,e:
                response = "Process Failed"

        self.write(response)

class domainsunique(tornado.web.RequestHandler):
    def get(self):
        start_date = self.get_argument('start_date', '1412719261')
        end_date = self.get_argument('end_date', '1412794861')
        dire = glob.glob("./data/*.json")
        dire = [x for x in dire if int(start_date) <= int(x.replace('.tsv','').split('-')[-1]) <= int(end_date) ]
        
        if start_date > end_date:
            response = "Error, you have mentioned wrong dates"
        else:
            try:
                sc =SparkContext()
                # reading files simultanetly
                
                result = sc.textFile(','.join(dire)) \
                .map(lambda line: line.split("\n")) \
                .map(lambda line: line[0].replace('"','')) \
                .map(lambda line: yaml.load(line))\
                .filter(lambda x: int(start_date) <= x["timestamp"] <= int(end_date)) \
                .map(lambda x: x["url"].split('/')[-2])\
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda x,y:x+y) \
                .filter(lambda x: x[1] == 1)\
                .count()

                response = { 'number of unique domains': result,
                         'start_date': start_date,
                         'end_date': end_date }
                sc.stop()
            except Exception,e:
                raise e 
        self.write(response)

class statsbrowser(tornado.web.RequestHandler):
    def get(self):
        start_date = self.get_argument('start_date', '0')
        end_date = self.get_argument('end_date', '1500000000')
        dire = glob.glob("./data/*.json")

        if start_date > end_date:
            response = "Error, you have mentioned wrong dates"
        else:
            try:
                sc =SparkContext()
                # reading files simultanetly
                
                result = sc.textFile(','.join(dire)) \
                .map(lambda line: line.split("\n")) \
                .map(lambda line: line[0].replace('"','')) \
                .map(lambda line: yaml.load(line))\
                .filter(lambda x: int(start_date) <= x["timestamp"] <= int(end_date)) \
                .map(lambda x: x["user_agent"]["browser_family"])\
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda x,y:x+y) \
                .collect()

                total = 0
                for item in result:
                    total += item[1]

                result = [(x[0], float(x[1]) * 100/ total) for x in result]


                response = { 'browser average': result,
                         'start_date': start_date,
                         'end_date': end_date }
                sc.stop()
            except Exception,e:
                raise e 
        self.write(response)

class statsos(tornado.web.RequestHandler):
    def get(self):
        start_date = self.get_argument('start_date', '0')
        end_date = self.get_argument('end_date', '1500000000')
        dire = glob.glob("./data/*.json")

        if start_date > end_date:
            response = "Error, you have mentioned wrong dates"
        else:
            try:
                sc =SparkContext()
                # reading files simultanetly
                
                result = sc.textFile(','.join(dire)) \
                .map(lambda line: line.split("\n")) \
                .map(lambda line: line[0].replace('"','')) \
                .map(lambda line: yaml.load(line))\
                .filter(lambda x: int(start_date) <= x["timestamp"] <= int(end_date)) \
                .map(lambda x: x["user_agent"]["os_family"])\
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda x,y:x+y) \
                .collect()

                total = 0
                for item in result:
                    total += item[1]
                    
                result = [(x[0], float(x[1]) *100 / total ) for x in result]


                response = { 'OS average': result,
                         'start_date': start_date,
                         'end_date': end_date }
                sc.stop()
            except Exception,e:
                raise e 
        self.write(response)

class statsdevice(tornado.web.RequestHandler):
    def get(self):
        start_date = self.get_argument('start_date', '0')
        end_date = self.get_argument('end_date', '1500000000')
        dire = glob.glob("./data/*.json")

        if start_date > end_date:
            response = "Error, you have mentioned wrong dates"
        else:
            try:
                sc =SparkContext()
                # reading files simultanetly
                
                result = sc.textFile(','.join(dire)) \
                .map(lambda line: line.split("\n")) \
                .map(lambda line: line[0].replace('"','')) \
                .map(lambda line: yaml.load(line))\
                .filter(lambda x: int(start_date) <= x["timestamp"] <= int(end_date)) \
                .map(lambda x: x["user_agent"]["mobile"])\
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda x,y:x+y) \
                .collect()

                total = 0
                for item in result:
                    total += item[1]
                    
                result = [(x[0], float(x[1]) *100 / total) for x in result]
                mobile = 0
                not_mobile = 0
                if result[0][0] == "false":
                    not_mobile = result[0][1]
                    mobile = result[1][1]
                else:
                    not_mobile = result[1][1]
                    mobile = result[0][1]

                response = { 'Device average': {"mobile" : mobile, "not_mobile": not_mobile},
                         'start_date': start_date,
                         'end_date': end_date }
                sc.stop()
            except Exception,e:
                raise e 
        self.write(response)
        
application = tornado.web.Application([
    (r"/users/unique/", usersunique),
    (r"/users/total/", usersdistinct),
    (r"/domains/unique/", domainsunique),
    (r"/domains/total/", domainsdistinct),
    (r"/stats/browser/", statsbrowser),
    (r"/stats/os/", statsos),
    (r"/stats/device/", statsdevice),
])
 
if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()