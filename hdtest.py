import h5py
import sys
import numpy as np
import datetime
from functools import wraps
import logging
import os
import sys

utcnow = datetime.datetime.utcnow
now = datetime.datetime.now

def config_logger(logpath):

	logpath_debug = logpath
	logpath_info = logpath + ".critical"

	fmt='%(asctime)s %(levelname)s %(message)s'

	logger = logging.getLogger('')
	logger.setLevel(logging.DEBUG)

	debug_handler = logging.FileHandler(logpath_debug)
	critical_handler = logging.FileHandler(logpath_info)

	debug_handler.setLevel(logging.DEBUG)
	critical_handler.setLevel(logging.INFO)

	formatter = logging.Formatter(fmt)

	debug_handler.setFormatter(formatter)
	critical_handler.setFormatter(formatter)

	logger.addHandler(debug_handler)
	logger.addHandler(critical_handler)

def archive(archpath,grpname,sweep,t,err,read_rate,dt_w,gb_w,dt_ch,gb_ch,dt_cl,gb_cl):
	
	write_rate = gb_w/float(dt_w)
	read_and_check_rate = gb_ch/float(dt_ch)
	delete_rate = gb_cl/float(dt_cl)


	with h5py.File(archpath,'a') as f:
		
		grp = f[grpname]

		grp['byte_errors'][sweep] = err
		grp['total_sweep_time'][sweep] = t
		grp['read_rate'][sweep] = read_rate
		grp['read_and_check_rate'][sweep] = read_and_check_rate
		grp['write_rate'][sweep] = write_rate
		grp['delete_rate'][sweep] = delete_rate

def config_archive(archpath, nsweeps):
	"""
	Create HDF datasets to hold nsweeps worth
	of performance data

	Assuming a 6 GB/s write speed upper bound (probably
	twenty times the write speed we can achieve), a sweep 
	will take over 1100, seconds. If we prepare for one
	million sweeps, it would take over thirty six years
	to fill
	"""

	with h5py.File(archpath,'a') as f:

		fmt = '%Y-%m-%d %H:%M'
		dstr= datetime.date.strftime(utcnow(),fmt)
		grp = f.create_group(dstr)

		# 1D arrays, one element per sweep
		grp.create_dataset('byte_errors',(nsweeps,),dtype='i32')
		grp.create_dataset('total_sweep_time', (nsweeps,),dtype='i32')
		grp.create_dataset('read_rate',(nsweeps,),dtype='float32')
		grp.create_dataset('read_and_check_rate',(nsweeps,),dtype='float32')
		grp.create_dataset('write_rate',(nsweeps,),dtype='float32')
		grp.create_dataset('delete_rate',(nsweeps,),dtype='float32')

		grp.attrs['units'] = 'GB/s'

	return dstr

def timed(fn):
	@wraps(fn)
	def wrapper(*args,**kwargs):
		t0 = now()
		ret = fn(*args,**kwargs)
		t1 = now()
		dt = float((t1-t0).total_seconds())
		return dt,ret
	return wrapper

@timed
def write(nfiles,block):
	files = []
	n=0
	for i in xrange(nfiles):
		name = testdir+'/__hdtest__%i'%(i)
		n+=1
		try:
			with h5py.File(name,'w') as f:
				f.create_dataset('test',data=block)
			files.append(name)
		except:
			msg = 'file write failed: %s' %(name)
			logging.critical(msg)
			n -= 1

	return files, n

@timed
def clear(files):
	"""
	delete files, and return the number
	of files actually deleted
	"""
	i = 0
	for name in files:
		try:
			os.remove(name)
		except:
			msg = 'file deletion failed: %s' %(name)
			logging.critical(msg)
			return i
		i += 1

	return i

@timed
def check_files(files,block):
	"""
	read back from files, and count the number
	of incorrect bytes. Return the number of
	byte errors, the number of files 
	successfully opened, and the average read speed
	"""

	n = 0
	for i,fname in enumerate(files):
		n+=1
		try:
			with h5py.File(fname,'r') as f:
				t0 = now()
				check = np.copy(f['test'])
				t1 = now()
				rate = blocklen/float((t1-t0).total_seconds)
		except:
			n-=1
			logging.critical('file read failed')
			rate = 0

		accum_rate += rate
		

		check = (check==block)
		correct_bytes = np.sum(check)
		byte_errors = len(check) - correct_bytes

	return byte_errors,n, accum_rate/n 

@timed
def sweep(nfiles,block):
	"""
	Do one write-check-clear cycle
	"""

		dt_write,files,n = write(nfiles,block)
		gb_write = 10*n*blocklen/1000.
		
		logging.debug('wrote %s GB in %s seconds')
			%(gb_write,dt_write)

		dt_check,byte_errors,n,read_rate = check_files(files,block)
		gb_check = 10*n*blocklen/1000.

		logging.debug('checked %s GB in %s seconds')
			%(gb_check,dt_check)
		
		dt_clear,n = clear(files)
		gb_clear = 10*n*blocklen/1000.

		logging.debug('cleared %s GB in %s seconds')
			%(gb_clear,dt_clear)

		return byte_errors,read_rate,dt_write,
				gb_write,dt_check,gb_check,dt_clear,
				gb_clear

if __name__ == '__main__':


	#-----User Parameters------

	blocklen =  10 # MB
	sweeplen = 100 # GB
	testdir = "/home/alex/hdtest"
	logpath = "/home/alex/hdtest/hdtest.log"
	archpath = "/home/alex/hdtest/performance.h5"
	maxlogsize = 1 #MB

	#----Internal setup----

	maxlogsize *= 1e6 # bytes

	tmp = np.random.randint(0,255,blocklen)
	block = np.array(tmp, dtype=np.uint8)

	nfiles = 1000 * int(sweeplen/float(blocklen))

	config_logger(logpath)
	grpname = config_archive(archpath)

	msg = 'started hdtest with blocksize %s MB and sweeplength %s GB'
		%(blocksize,sweeplen)
	logging.info(msg)

	i = 0
	while True:

		t,err,read_rate,dt_w,gb_w,dt_ch,gb_ch,dt_cl,gb_cl=sweep(nfiles,block)

		try:
			archive(archpath,grpname,t,i
				err,read_rate,
				dt_w,gb_w,dt_ch,
				gb_ch,dt_cl,gb_cl)
		except:
			# could be because the archive is full, but that
			# can be rather hard to do
			logging.critical('performance archive nonresponsive')
			sys.exit(1)

		# clear the debug log if it gets too big
		debug_size = os.path.getsize(logpath_debug)
		if debug_size >= maxlogsize:
			with open(logpath_debug,'w') as f:
				pass

		i += 1












