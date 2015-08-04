import numpy as np
import time
import random

def nohup():
	signal.signal(signal.SIGHUP, signal.SIG_IGN)
	
class hdtest(object):
	
	def __init__(self, root, log, maxlen, minlen):
		
		self.root = root
		self.maxlen = maxlen # TB
		self.minlen = minlen
		self.chunk = None
		self.sweeplen = None
		self.tree = None
		self.flen = 0.001
		
		fmt='%(asctime)s %(levelname)s %(message)s'	
		logging.basicConfig(filename=log,format=fmt,level=logging.INFO)
		
		self.clear_tree()
		
		
	def make_tree(self):
		
		ndirs = 100
		nfiles_per_dir = 100
		
		nfiles = int(self.sweeplen / self.flen)
		
		tree = []
		i = 0
		for d in ndirs:
			
			dirname = os.path.join(self.root,str(d))
			os.makedir(dirname)
			
			for f in nfiles_per_dir:
				
				if i == nfiles:
					self.tree = tree
					return
									
				fname = os.path.join(dirname,str(f))
				tree.append(fname)
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
		
		nelements = int(1e12 * flen/4.0)
		self.chunk = np.random.random(nelements)
		
		self.make_tree()
		
		failures = 0
		
		t0 = time.time()
		
		for f in tree:
			
			try:
				np.save(chunk,f)
			except:
				failures += 1
		
		t1 = time.time()
		speed = self.sweeplen * 1e6 / (t1-t0) 
		
		msg = "wrote %.2f TB at .2f MB/s" % (self.sweeplen,speed)
		msg += "\t failed file writes = %i\n" %(failures)
		
		logging.info(msg)
		
	def check(self):
		
		self.get_tree()
		
		word_errors = 0
		file_errors = 0
		
		t0 = time.time()
		
		for f in tree:
			
			try:
				arr = np.loads(f)
			except:
				file_errors += 1
				
			word_errors += np.sum(arr==self.chunk)
			
		t1 = time.time()
		speed = self.sweeplen * 1e6 / (t1-t0) 
		
		msg = "read %.2f TB at .2f MB/s" % (self.sweeplen,speed)
		msg += "\t failed file reads = %i\n" %(file_errors)
		msg += "\t word errors = %i\n" %(word_errors)
		
		logging.info(msg)
		
		
		
		
