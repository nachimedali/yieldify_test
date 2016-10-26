# Data Engineering Test 
# Yieldify

# Thanking
I would like, at first, to thank you for your time and consideration. This is the kind of project and challenges that I am looking for, and hope to be selected for this position.

# Answers
## Test 1 : Design
### Overall System Design
We can design that system by getting User agent from user as an asynchronous task. In other terms, when a user connect to website via his browser, we can get and parse its agent and save it into db. We need to use an asynchronous task to not bother user with this task.

For an asynchronous task, we turn to Javascript (in my example, I have used AngularJS). a simple script can be included in webpage to get and parse the user agent, and send data retrieved to backend to store in db.

This figure would resume the process:
![alt text][logo]
[logo]: ./images/task_1.png "process"

1. Task 1 : User connects to website 
2. Task 2 : Adding a Javascript script to webpage that collect User Agent data (and optionally parse it as it can be parse in the backend)
3. Task 3 : When data received, we need to proceed to parsing if it is not already done, formatting data and preparing the data structure that we want (Json, csv)
4. Task 4 : Once data collected, tested and formatted, we can store then data to our data storage system.

#### Components that need to be developed
- This is what it is required for A
1. The script that wouldbe integrated in our webpage that retrieves User Agent data
2. Parsing system with regex to extract required data
- Passing data from A to B
3. Backend view that would receive data from javascript via HTTP, and proceed to storage
- A webservice:
4. The service/API that would get requests from users and generate responses

I have generated a simple example of this task with only javascript that generates JSON file with data required. Here is the link : [Simple Example to parse user agent data](http://yieldify.alwaysdata.net)
I have used:
- AngularJS
- ua-parser-js lib [Link](https://github.com/faisalman/ua-parser-js)

### Physical system Design
AWS proposes multiple tools for different requirements:
S3 : is a storage system, a bucket made to store files, but it can not be used to deploy services or API 
EC2 : is a Cloud and hosting system for apps, and that supports obviously webservices and APIs. 

To proceed to deployment of a such system, we need to use:
S3 : to store our data, user agents data or and data required for the system
EC2 : to deploy our web application and run services. After deploying our app on EC2, our app would handle request from user from its browser, and backend would pass different HTTP request to and from S3 bucket to generate requested data and returned to user. This design is also true for APIs.

This figure would resume the process:
![alt text][logo]
[logo]: ./images/task_2.png "process"

1. The user, via his browser, would connect to our web application.
2- The Application deployed on AWS EC2 (Django, Node ...) would handle different requests from user. If response do not require any data stored in AWS S3, the app would render a response without interacting with S3 Bucket. Otherwise, depending on the app, it generate different request to S3 bucket to add, delete, update data stored and got response.

And to adapt this system to our requirements:
1. A user connects to our web application.
2. The app deployed in EC2 would get and parse his User Agent data, format it and generate a file to store
3. Once data is prepared to be stores, a request would be sent to S3 bucket and send this data to be stored in the bucket.
4. A response would be received in app deployed in EC2 and would be rendred to user (if needed to show him that we have collected his data)

This is the same approach for an API:
1. A request would be received by the app deployed on EC2
2. The app would handle the request, to see if he have credential and to define data required 
3. A request would be sent to S3 bucket to get needed data
4. once received, data would be formatted and sent back to user 

## Part 2 : Implementation
### Task 1 : Creating a service

