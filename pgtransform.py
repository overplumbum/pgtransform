#!/usr/bin/env python
from struct import unpack, calcsize
import sys
import zlib


def __funpack(fmt, f):
	sz = calcsize(fmt)
	buf = f.read(sz)
	if sz and buf == '':
		return None,
	return unpack(fmt, buf)


def __get_string(f):
	len = __get_int(f)
	if len == -1:
		return None
	elif len == 0:
		return ''
	else:
		return __funpack(str(len)+'s', f)[0]


def __get_int(f):
	sign = __funpack('b', f)[0]
	assert sign in (0, 1), sign
	value = __funpack('<i', f)[0]
	if -sign:
		return -value
	else:
		return value

		
def read_head(f):
	assert __funpack('5s', f)[0] == 'PGDMP'
	vmaj, vmin, vrev, intSize, offSize, format = __funpack('bbbbbb', f)
	assert vmaj >= 1
	assert vmin >= 10
	assert intSize == 4
	assert offSize == 8
	assert format == 1

	compression = __get_int(f)
	assert compression in range(10), compression

	tm_sec, tm_min, tm_hour, tm_mday, tm_mon, tm_year, tm_isdst = [__get_int(f) for i in range(7)]
	print 'created:', tm_sec, tm_min, tm_hour, tm_mday, tm_mon, tm_year, tm_isdst
	db = __get_string(f)
	remoteVersionStr = __get_string(f)
	pg_version = __get_string(f)
	print db, remoteVersionStr, pg_version
	

def read_toc(f):
	toc = {}
	tocCount = __get_int(f)

	for i in range(tocCount):
		dumpId = __get_int(f)
		te = {}
		dataDumper = __get_int(f)
		assert dataDumper in (0, 1)

		catalog_tableoid = __get_string(f)
		catalog_oid = __get_string(f)
		
		tag, desc = __get_string(f), __get_string(f)
		te['tag'] = tag
		section = __get_int(f)
		
		defn, dropStmt, copyStmt, namespace, tablespace, owner, withOids = [__get_string(f) for j in range(7)]
		assert withOids in ('true', 'false')
		
		if copyStmt:
			tmp, stmt = copyStmt.split('(', 1)
			assert tmp.startswith('COPY ')
			stmt, tmp = stmt.rsplit(')', 1)
			assert tmp.strip().endswith('FROM stdin;'), tmp
			te['copy_attrs'] = stmt.split(', ')

		
		deps = []
		while True:
			dep = __get_string(f)
			if dep is None:
				break
			deps.append(dep)

		offset_wasSet = __funpack('b', f)[0]
		offset_smallest_first = __funpack('<Q', f)[0]
		
		toc[dumpId] = te
		print 'toc entry#', i, namespace, tag
	
	return toc

		
def read_data_chunks(f, toc, transformations):
	while True:
		kind = __funpack('b', f)[0]
		if kind is None:
			break
		assert kind in (1, 3), kind

		id = __get_int(f)
		te = toc[id]
		print 'id', id, 'tag', te['tag']
		
		if kind == 3:
			_LoadBlobs(AH, ropt.dropSchema);
		elif kind == 1:
			buf = []
			while True:
				blkLen = __get_int(f)
				if blkLen != 4096:
					print 'blkLen', blkLen
				if not blkLen:
					break
				buf.append(f.read(blkLen))
			
			trans = transformations.get(te['tag'])
			if trans:
				buf = zlib.decompress(''.join(buf))
				for line in buf.splitlines():
					if line != r'\.' and line != '':
						row = line.split('\t')
						assert len(row) == len(te['copy_attrs']), line
						row = dict(zip(te['copy_attrs'], row))
						trans(row)
						print row
					else:
						print 'done'
						

def run(input, transformations):
	f = open(input, 'r')
	read_head(f)
	toc = read_toc(f)
	read_data_chunks(f, toc, transformations)


__ALL__ = [run]
