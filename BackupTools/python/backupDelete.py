#!/usr/bin/env python3

from boto3.s3.transfer import S3Transfer
from boto3.s3.transfer import TransferConfig
from sys import stdin
from binascii import hexlify
import logging
from threading import Thread
from queue import Queue
import time
import boto3
import grp
import pwd
import os
import mimetypes


class UploadWorker(Thread):

	def __init__(self, queue):
		Thread.__init__(self)
		self.queue = queue

	def run(self):
		# Setup the connection to the S3 container
		client = boto3.client (
        		's3',
        		use_ssl=False,
        		endpoint_url='http://igneousdemo.iggy.bz/',
        		aws_access_key_id='REXZF8RVPQ5DJQOC1NP2',
        		aws_secret_access_key='oLwHMsGUugSGtFowU8DPE0xsZ4jB84fqt980oTGk'
		)

		# Setup the parameters for the upload 
		config = TransferConfig(
        		multipart_threshold= 20 * 1024 * 1024 * 1024,  # Set threshold to 8GB before using Multipart
        		max_concurrency=10,
		)
		# setup the way to transfer 
		transfer = S3Transfer(client, config)

		while True:
			# Get the work from the queu and expand the tuple
			directory, link = self.queue.get()
			upload_file(directory, link, transfer)
			self.queue.task_done()


def upload_file(dirName, fileName, transfer):

	fullFileName = '{0}/{1}'.format(dirName,fileName)
	fullFileNameFixed = fullFileName[1:]

	try:
		if os.path.isfile(fullFileName):
			statInfo = os.stat(fullFileName)
			mimeType, encType = mimetypes.guess_type(fullFileName, False)
			if mimeType is None: 
				mimeType = "Unknown"
			if encType is None:
				encType = "Unknown"
			print("File mimetype : {0}".format(mimeType))	
			print("File to upload : {0}".format(fullFileNameFixed))
			transfer.upload_file(fullFileName, 'multimedia-commons', fullFileNameFixed,
				extra_args={'Metadata': {'backup-date'      : time.ctime(), 
							 'meta-ign-u-mode'  : str(statInfo.st_mode),
							 'meta-ign-u-uid'   : str(statInfo.st_uid),
							 'meta-ign-u-gid'   : str(statInfo.st_gid),
							 'meta-ign-c-ctime' : str(statInfo.st_atime), 
							 'meta-ign-c-mtime' : str(statInfo.st_mtime),
							 'meta-ign-c-ctime' : str(statInfo.st_ctime),
							 'meta-ign-mimetype': mimeType
							 }}) 
			os.remove(fullFileName)
		else:           
			directoryName = '{0}/'.format(dirName[1:])
			print("Directory to create : {0}".format(directoryName))
			statInfo = os.stat(dirName)
			transfer.upload_file('/tmp/zerofile.0', 'multimedia-commons', directoryName,
				extra_args={'Metadata': {'backup-date': time.ctime(),
							 'meta-ign-u-mode'  : str(statInfo.st_mode),
							 'meta-ign-u-uid'   : str(statInfo.st_uid),
							 'meta-ign-u-gid'   : str(statInfo.st_gid),
							 'meta-ign-c-ctime' : str(statInfo.st_atime), 
							 'meta-ign-c-mtime' : str(statInfo.st_mtime),
							 'meta-ign-c-ctime' : str(statInfo.st_ctime)
                                           }}) 
			os.rmdir(dirName)
                                       
	except IOError as e:
		print("I/O error({0}):{1}".format(e.errno, e.strerror))


def main(): 
	ts = time.time()
	rootDir = '/mnt/data/createdata/multimedia-commons'

	try:
       		file = open ('/tmp/zerofile.0', 'w+')
        	file.close()
	except:
        	print('Something went wrong with create small file')
        	sys.exit(0)

	queue = Queue()

	for x in range(10):
		worker = UploadWorker(queue)
		#Setting the daemon to True will let the main thread exit even though the worker is not done
		worker.daemon = True
		worker.start()


	for dirName, subdireList, fileList in os.walk(rootDir, topdown=True):
		queue.put((dirName, ""))
		for fname in fileList:
			queue.put((dirName, fname))

	queue.join()

	print('Took {}'.format(time.time() - ts))


if __name__ == '__main__':
	main()


