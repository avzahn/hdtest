import numpy as np
import time
import random
import logging
import signal
import os

def set_nohup():
	signal.signal(signal.SIGHUP, signal.SIG_IGN)
	
class hdtest(object):
	
	def __init__(self, root, log, maxlen, minlen=None,flen=.0001):
		
		self.root = root
		self.maxlen = maxlen # TB
		if minlen == None:
			self.minlen = .8 * maxlen
		else:
			self.minlen = float(minlen)
		self.chunk = None
		self.sweeplen = None
		self.tree = None
		self.flen = float(flen)
		
		fmt='%(asctime)s %(levelname)s %(message)s'	
		logging.basicConfig(filename=log,format=fmt,level=logging.INFO)
		
		self.clear_tree()
		
		
	def make_tree(self):
		

		nfiles_per_dir = 1000
		
		nfiles = max(1,int(self.sweeplen / self.flen))
		ndirs = max(1,nfiles/nfiles_per_dir)		
		self.tree = []

		print"%i total files over %i directories to span %.2f TB"%(nfiles,ndirs,self.sweeplen)
		i = 0
		for d in range(ndirs):
			
			dirname = os.path.join(self.root,str(d))
			os.mkdir(dirname)
			
			for f in range(nfiles_per_dir):
				
				if i == nfiles:
					return
									
				fname = os.path.join(dirname,str(f)+".npy")
				self.tree.append(fname)
				i += 1
				
	def clear_tree(self):
		
		top = self.root
		for root, dirs, files in os.walk(top, topdown=False):
			for name in files:
				os.remove(os.path.join(root, name))
			for name in dirs:
				os.rmdir(os.path.join(root, name))
				
	def get_tree(self):
		
		tree = []
		
		top = self.root
		
		for root, dirs, files in os.walk(top, topdown=False):
			for name in files:
				tree.append(os.path.join(root, name))
				
		self.tree = tree
	
				
	def fill(self):
		
		self.sweeplen = self.minlen + (self.maxlen-self.minlen)*random.random()
		
		nelements = int(1e12 * self.flen/4.0)
		self.chunk = np.random.random(nelements)
		
		print "%.3e" % ( nelements)


		failures = 0
		
		self.make_tree()

		t0 = time.time()

		files_written = 0
		
		for f in self.tree:
			
			try:
				np.save(f,self.chunk)
				files_written += 1
			except Exception as e:
				failures += 1
				print e
		
		t1 = time.time()
		speed = self.sweeplen / (1e6*(t1-t0)) 
		
		msg = "\n\twrote %.2f TB at %.2f MB/s" % (files_written * self.flen,speed)
		msg += "\n\t\t failed file writes = %i\n\n" %(failures)
		
		logging.info(msg)
		
	def check(self):

		
		word_errors = 0
		file_errors = 0
		
		t0 = time.time()

		self.get_tree()
		

		files_read = 0
		
		for f in self.tree:
			
			try:
				arr = np.loads(f)
				files_read += 1
			except:
				file_errors += 1
				
			word_errors += np.sum(arr==self.chunk)
			
		t1 = time.time()
		speed = self.sweeplen / (1e6 * (t1-t0)) 



		msg = "\nread %.2f TB at %.2f MB/s"%(files_read*self.flen,speed)
		msg += "\n\t\t failed file reads = %i\n" %(file_errors)
		msg += "\n\t\t word errors = %i\n\n" %(word_errors)
		
		logging.info(msg)
		
	def test(self,n=None,nohup=True):

		if nohup:
			set_nohup()
		if n == None:
			while True:
				self.clear_tree()
				self.fill()
				self.check()

		else:
			for i in range(n):
				self.clear_tree()
				self.fill()
				self.check()	

if __name__ == "__main__":
	hd = hdtest("/mnt/helium/test","hdtest.log",6.5)
	hd.test()
