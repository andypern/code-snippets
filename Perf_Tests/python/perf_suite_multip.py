#!/usr/bin/env python

#This is an attempt at using python's multiprocessing module 
# to parallelize puts/gets: working around the GIL associated w/ using the threading module.

import boto3
import botocore
from boto3.s3.transfer import S3Transfer, TransferConfig

import time

import sys
import os
import getopt
from string import ascii_uppercase
from random import choice
import xml.etree
import pprint
from datetime import datetime
import logging



from multiprocessing import Process, JoinableQueue, Queue
from progressbar import ProgressBar, SimpleProgress




# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# logging.getLogger('requests').setLevel(logging.CRITICAL)
# logger = logging.getLogger(__name__)





#####
#some variables/defaults
#
print_verbose = False
ramp = False
filecount = 20
threadcount = 5
fileprefix = "file-"
#
#
####


try:
        opts, args = getopt.getopt(sys.argv[1:], "e:a:s:b:i:f:t:dv", 
            ["endpoint","access_key=","secret_key=","bucket=","inputfile=","filecount=",
            "threadcount=","ramp","delete","verbose"])
except getopt.GetoptError as err:
        # print help information and exit:
        print(err) # will print something like "option -a not recognized"
        print "wrong option"
        sys.exit(2)


for opt, arg in opts:

    if opt in ('-e', '--endpoint'):
        endpoint = arg
    if opt in ('-a', '--access_key'):
        access_key = arg
    if opt in ('-s','--secret_key'):
        secret_key = arg
    if opt in ('-b','--bucket'):
        bucket = arg
    if opt in ('-i','--inputfile'):
        inputfile = arg
    if opt in ('-f','--filecount'):
        filecount = int(arg)
    if opt in ('-t','--threadcount'):
        threadcount = int(arg)
    if opt in ('-r','--ramp'):
        ramp = True
    if opt in ('-d','--delete'):
        delete_only = True
    if opt in ('-v', '--verbose'):
        print_verbose = True

#
#we require 5 arg's, and 2 are optional.
#

if len(opts) < 5:
    print "syntax is `./perf_suite.py -e <endpoint> -a <access_key> -s <secret_key> -b bucket -i inputfile [-c filecount] [--ramp] [--delete] [--verbose]`"
    sys.exit(1)


class fileUploader(Process):

    def __init__(self, file_queue, result_queue, endpoint, bucket, access_key, secret_key):
        Process.__init__(self)
        self.file_queue = file_queue
        self.result_queue = result_queue
        self.endpoint = endpoint
        self.bucketname = bucket
        self.access_key = access_key
        self.secret_key = secret_key

    def run(self):
        proc_name = self.name

        transfer, client, config = make_session(self.endpoint, self.access_key, self.secret_key)
        
        while True:
            next_task = self.file_queue.get()
            print("{}: Queue String: {}".format(proc_name,next_task))
        
            if next_task is None:
                self.file_queue.task_done()
                break
            #print("file_name {}".format(next_task[0]))
            
            
            upload_result = upload_file(self.bucketname, next_task,transfer)
            self.result_queue.put(upload_result)

        #print("{}: Queue String: {}".format(proc_name,next_task))
        #print("Exiting {}".format(proc_name))
        return



def make_session(endpoint,access_key,secret_key):

    session = boto3.session.Session()

    try:


        client = session.client('s3',
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key,
            endpoint_url='http://' + endpoint,
            use_ssl=False,
            verify=False,
            config=boto3.session.Config(
                signature_version='s3',
                connect_timeout=60,
                read_timeout=60,
                s3={'addressing_style': 'path'})
                )

        config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=20 * 1024 * 1024 * 1024, 
            max_concurrency = 10,
        )

        transfer = boto3.s3.transfer.S3Transfer(client, config)

    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        print("Errorcode {} Connecting to {} : with access key {} : secret key {}".
            format(error_code, endpoint, access_key, secret_key))
        sys.exit(1)

    return transfer,client,config


def upload_file(bucket, objKey, transfer):

    print "%s in upload_file" %(objKey)
    local_start_time = time.time()
    try:    
        response = transfer.upload_file(
            inputfile, 
            bucket,
            objKey
            )
        local_elapsed_time = time.time() - local_start_time
        #print "at %s made file # %s in %s" %(nowtime, i, local_elapsed_time)
    except botocore.exceptions.ClientError as e:
        printfail(method,e.response)
    except (botocore.vendored.requests.exceptions.ConnectionError, 
        botocore.vendored.requests.exceptions.ReadTimeout) as e:
        #if we get a timeout, retry until we hit max_retries
        print "got a timeout for %s: %s" %(objKey,e)
        local_elapsed_time = time.time() - local_start_time
        print "this timeout took %s , initiating retry" %(local_elapsed_time)

        for attempt in range(max_retries):
            print "attempt #%s of %s" %(attempt + 1,max_retries)
            try:
                response =  transfer.upload_file(
                    inputfile, 
                    bucket,
                    objKey
                    )
                local_elapsed_time = time.time() - local_start_time
                #print "at %s made file # %s in %s" %(nowtime, i, local_elapsed_time)
                break
            except (botocore.vendored.requests.exceptions.ConnectionError, 
                    botocore.vendored.requests.exceptions.ReadTimeout) as e:
                if attempt < (max_retries - 1):
                    print "attempt %s failed, %s remaining" %(attempt + 1, max_retries - attempt)
                    continue
                else:
                    print "all attempts failed..aborting this transfer"
                    return objKey + ",bad"
                    break

    return objKey + ",good"

def delete_object(s3client,bucket,objKey):
    method = 'delete_object'
    
    try:
        response = s3client.delete_object(
            Bucket=bucket,
            Key=objKey
        )

    except botocore.exceptions.ClientError as e:
        printfail(method,e.response)



def list_objects(s3client,bucket, fileprefix):       
    #
    #note that you can only get back up to 1000 keys at a time
    # so using pagination to try and get them all.
    #
    try:
        paginator = s3client.get_paginator('list_objects')

        pages = paginator.paginate(
            Bucket=bucket,
            #Delimiter='/',
            Prefix=fileprefix
        )
        print bucket
        return pages
    except botocore.exceptions.ClientError as e:
        printfail(method,e.response)






def printfail(method,response):
    #
    #this is mainly so we have a consistent way to print 
    #
    # for later: if there is the '-v' flag set by the user, print more verbose stuff.
    #
    if print_verbose == True:
        print "%s,%s" % (method,response)
    else:
        print "%s,%s" % (method,response['ResponseMetadata']['HTTPStatusCode'])







def main():
    #check to make sure input file exists

    try:
        fd = open (inputfile, 'r')
        fd.close()
    except IOError as e:
        print'couldnt open %s : %s' %(inputfile,e)
        sys.exit(0)



    #figure out how big the file is, we care for later
    fsize = os.stat(inputfile).st_size

    ts = time.time()

    file_queue = JoinableQueue()
    result_queue = Queue()

    file_processes = [ fileUploader(file_queue, result_queue, endpoint, bucket, access_key, secret_key)
                      for i in range(threadcount) ]    

    fileList = []
    for f in range(filecount):
        objKey = fileprefix + str(f) + '-' + (''.join(choice(ascii_uppercase) for i in range(6)))
        fileList.append(objKey)



    for w in file_processes:
        w.start()

    start_time = time.time()
    
    for objKey in fileList:
        file_queue.put(objKey)
      
     #
    #this is sort of ghetto..add a None to the queue so that when processing we can tst for it to know 
    # that we're at the end of the queue..must be a better way
    #

    for i in range(threadcount):
        file_queue.put(None)

    print("Waiting for threads to stop")
    for w in file_processes:
        w.join()

        delta = time.time() - ts
    print('Took {}'.format(delta))

    #
    #rough calculations
    #
    totalBytes = fsize * filecount
    bytesSec = totalBytes / delta
    MBytesec = totalBytes / (1024 * 1024) / delta
    print "%s MB/sec" %(MBytesec)

    #now delete
    #
    #get a list of objects in pages, then blow them away
    #
    s3client = make_session(endpoint, access_key, secret_key)[1]

    objList =  list_objects(s3client,bucket, fileprefix)

    count = 0
    for page in objList:
            for key in page['Contents']:
                delete_object(s3client,bucket,key['Key'])
                count += 1


    print "deleted %s" %(count)



if __name__ == '__main__':
    main()