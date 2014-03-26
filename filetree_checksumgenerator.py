#!/usr/bin/python

# Imports
import boto
import csv
import datetime
import hashlib
import logging
import os
import sys
import time

from optparse import OptionParser

parser = OptionParser()
parser.add_option('-c', '--check', dest = 'check_flag', help = 'Verify checksum.',default=False,action='store_true')
parser.add_option('-w', '--write', dest = 'write_flag', help = 'Generate checksum.',default=False,action='store_true')
parser.add_option('-p', '--path', type = 'string', dest = 'path_to_parse', help = 'Please enter path to the tree.')
parser.add_option('-f', '--filename', type = 'string', dest = 'mdf5_file_name', help = 'Name to use for your hash file.',default='files.md5')
parser.add_option('-t','--topic',type='string',dest='topic_arn',help='Please specify the topic arn')
(options, args) = parser.parse_args()

if options.check_flag is False and options.write_flag is False:
    logging.error('Please specify -c or -w to check or write files respectively.')
    parser.print_help()
    sys.exit(-1)

if options.check_flag is True and options.write_flag is True:
    logging.error('Please specify only one of -c or -w to check or write files respectively.')
    parser.print_help()
    sys.exit(-1)

if options.path_to_parse is None or not os.path.isdir(options.path_to_parse):
    logging.error('Please specify a valid path to parse.')
    parser.print_help()
    sys.exit(-1)
    
if options.topic_arn is None:
    logging.error('Please specify a valid topic arn.')
    parser.print_help()
    sys.exit(-1)

def md5_for_file(name, block_size=2**20):
    f= open(name, 'rb')
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    f.close()
    return md5.hexdigest()

def send_msg_aws(topic_arn,mesg):
    # Amazon Resource Name
    c = boto.connect_sns()
    c.publish(topic_arn,mesg,"Files Status")

if options.check_flag:
    content = ''
    for root, dirs, files in os.walk(options.path_to_parse):
        if os.path.exists(root + "/"+ options.mdf5_file_name):
            f = open(root + "/"+ options.mdf5_file_name, 'r')
            data = csv.reader(f,delimiter='\t')
            for row in data:
                if md5_for_file(root + "/"+row[0]) == row[2]:
                    print "File hasn`t changed!"
                else:
                    print "File has changed."
                    content = content + root + "/"+row[0]+" has changed "+"\n"
            f.close()
    if not content == '':
        send_msg_aws(options.topic_arn,content)

if options.write_flag:
    now = datetime.datetime.now()
    ts = time.time()
    for root, dirs, files in os.walk(options.path_to_parse):
        if len(files) > 0:
            skip_filenames = []
            if os.path.exists(root + "/"+ options.mdf5_file_name):
                f = open(root + "/"+ options.mdf5_file_name, 'r')
                data = csv.reader(f,delimiter='\t')
                skip_filenames = [str(row[0]) for row in data]
                f.close()
            f = open(root + "/"+ options.mdf5_file_name, 'a')
            writer = csv.writer(f,delimiter='\t')

            for name in files:
                if name not in skip_filenames and name != options.mdf5_file_name:
                    writer.writerow([name,os.path.getmtime(root + "/"+name),md5_for_file(root + "/"+name),str(time.time())])
            f.close()
