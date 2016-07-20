#!/usr/bin/env python


#
#This script illustrates how to connect using the boto3 library to an Igneous Data Router.
# Upon successful connection, it will print out a list of containers (buckets)
#
import boto3

import os
import sys
from botocore.exceptions import ClientError
import ConfigParser
import argparse




###Functions
#
#
#

	#
	#build the client session. 
	#


def make_session(endpoint,accessKey,secret_key,use_ssl):

	session = boto3.session.Session()

	
	s3client = session.client('s3',
			aws_access_key_id = accessKey,
	        aws_secret_access_key = secretKey,
			endpoint_url=endpoint,
			#region_name is needed for s3v4. can be pretend.
			region_name="iggy-1",
			use_ssl=use_ssl,
			verify=False,
			config=boto3.session.Config(
				#set signature_version to either s3 or s3v4
				#note: if you set to s3v4 you need to set a 'region_name'
				signature_version='s3',
				connect_timeout=60,
				read_timeout=60,
				#addressing style must be set to 'path'
				s3={'addressing_style': 'path'})
			)

	return s3client


def getArgs():

	parser = argparse.ArgumentParser(description = "connect and list buckets")

	parser.add_argument('-a', '--access_key', dest="accessKey",
	help = "Igneous access key")

	parser.add_argument('-s','--secret_key', dest="secretKey",
	help = "Igneous secret key")

	parser.add_argument('-e', '--endpoint',
	help = "Igneous endpoint, eg: http://igneous.company.com:80")


	parser.add_argument('-u', '--use_ssl',
	dest='use_ssl',
	default=False,
	help = "use ssl..or not, default is not")



	args =  parser.parse_args()



	return args.endpoint,args.accessKey,args.secretKey,args.use_ssl


#
#
#
#####End Functions

if __name__ == "__main__":

	endpoint,accessKey,secretKey,use_ssl = getArgs()


	s3client = make_session(endpoint,accessKey,secretKey,use_ssl)


	try:
		response = s3client.list_buckets()
	except ClientError as e:
		print e.message
		sys.exit(0)


	#
	# Just print list of bucket names
	#
	for bucket in response['Buckets']:
		print bucket['Name']
		
