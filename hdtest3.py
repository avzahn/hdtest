#! /home/polarbear/anaconda/bin/python

import datetime
from functools import wraps
import logging
import os
import signal
import numpy as np
import time

def nohup():
	signal.signal(signal.SIGHUP, signal.SIG_IGN)

def wait(s):
	start = datetime.datetime.now()
	while True:
		time.sleep(s/10)
		now = datetime.datetime.now()
		dt = (now-start).total_seconds() 
		if dt >= s:
			return dt
	
def config_log0(log0):
	fmt='%(asctime)s %(levelname)s %(message)s'	
	logging.basicConfig(filename=log0,format=fmt,level=logging.INFO)
	
def smartctl_test(drive,log1):
	with open(log1,"a") as f:
		f.write("#### smartctl %s ####\n "%(time.strftime("%H:%M:%S")))
		
	logging.info("starting smartctl long test")
	
	msg = "smartctl --test=long  %s >> %s 2>&1" %(drive,log1)
	system(msg)
	
	wait(6 * 3600)
	
	msg = "smartctl -a %s >> %s" %(drive,log1)
	system(msg)
	
def clear(TB,flen,testdir):
	nfiles = int(1000*(TB/float(flen)))
	for i in xrange(nfiles):
		fname = testdir+"/__hdtest__"+str(i)+".npy"
		os.remove(fname)

def sweep(TB,flen,testdir):
	
	#flen GB
	
	nfiles = int(1000*(TB/float(flen)))
	
	nelements = int(1e9 * flen/4.0)
	
	chunk = np.random.random(nelements)
	
	failed_file_writes = 0
	failed_file_reads = 0
	word_errors = 0
	
	tr = 0
	tw = 0
	
	logging.info("starting write")
	
	for i in xrange(nfiles):
		
		fname = testdir+"/__hdtest__" + str(i)
		
		t0 = datetime.datetime.now()
		try:
			np.save(fname,chunk)
		except:
			failed_file_writes += 1
			wait(100)
		
		tw += (datetime.datetime.now() - t0).total_seconds()
		
	logging.info("starting read")
		
	for i in xrange(nfiles):

		fname = testdir+"/__hdtest__" + str(i)
		
		t0 = datetime.datetime.now()
		try:
			arr = np.load(fname)
		except:
			failed_file_reads += 1
			wait(100)
			continue
		
		tr += (datetime.datetime.now() - t0).total_seconds()
		
		word_errors += np.sum(arr==chunk)
		
		del arr
		
	msg = "swept %f TB: \n\t write time = %i \n\t read time = %i \n\t failed file writes = %i \n\t failed file reads = %i \n\t word errors = %i" \
		%(TB,tw,tr,failed_file_writes,failed_file_reads,word_errors)
	logging.info(msg)
	
	
if __name__ == "__main__":
	
	import random

	log0 = "/home/polarbear/hdtest_sweep.log"
	log1 = "/home/polarbear/hdtest_smartctl.log"
	
	drive = "/dev/sdb1"
	testdir = "/mnt/helium/test"
	flen = 1.0

	"""
	log0 = "/home/alex/hdtest/sweep.log"
	log1 = "/home/alex/hdtest/smartctl.log"
	
	drive = "/dev/sda1"
	testdir = "/home/alex/hdtest/test"
	flen = 0.05
	"""
	
	nohup()
	config_log0(log0)
	
	#while True:
	for j in range(3):
		
		for i in range(5):
			#TB = 6.0 + (random.random()-0.5)
			TB = 0.01
			sweep(TB,flen,testdir)
			clear(TB,flen,testdir)
		smartctl_test(drive,log1)
			
		
		
	
	
	
	
	
	
	
		
		
		
		
		
		
		
		
		
		
		
		
	
	
