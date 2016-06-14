####
# This sample is published as part of the blog article at www.toptal.com/blog
# Visit www.toptal.com/blog and subscribe to our newsletter to read great posts
####

import logging
import os
from functools import partial
from multiprocessing.pool import Pool
from time import time

container = "apcontainer"

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('requests').setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)

def runstuff(bucket,linkaroo):
    print "%s -> %s" %(linkaroo,bucket)

def main():
    ts = time()
    bucket = container
    links = ['www.google.com','www.yahoo.com']
    download = partial(runstuff,container)


    p = Pool(processes=8)
    p.map(download, links)
    logging.info('Took %s seconds', time() - ts)


if __name__ == '__main__':
    main()