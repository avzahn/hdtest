import os
import logging
import time
import signal
from os import system

def config_log0(log0):
	fmt='%(asctime)s %(levelname)s %(message)s'	
	logging.basicConfig(filename=log0,format=fmt,level=logging.INFO)

def badblocks(drive,log1):
	with open(log1,"a") as f:
		f.write("#### badblocks %s ####\n "%(time.strftime("%H:%M:%S")))
	logging.INFO("starting badblocks -wv")
	msg = "badblocks -wv %s >> %s 2>&1" %(drive,log1)
	system(msg)

def smartctl_scan(drive,log1):
	with open(log1,"a") as f:
		f.write("#### smartctl %s ####\n "%(time.strftime("%H:%M:%S")))
		
	logging.INFO("starting smartctl long test")
	
	msg = "smartctl -C --test=long  %s >> %s 2>&1" %(drive,log1)
	system(msg)
	
def smartctl_status(drive,log1):
	
	msg = "smartctl -a %s >> %s" %(drive,log1)
	system(msg)
	
def nohup():
	signal.signal(signal.SIGHUP, signal.SIG_IGN)
	
if __name__ == "__main__":
	
	log0 = "hdtest.shortlog"
	log1 = "hdtest.log"
	
	drive = "/dev/sdb"
	
	config_log0(log0)
	nohup()
	
	while True:
		
		smartctl_status(drive,log1)
	
		for i in range(5):
			ok = badblocks(drive,log1)
			
			if ok != 0:
				time.sleep(3600)
			
		smartctl_scan(drive,log1)
		
		
	
