import glob, os, sys, math
#directory = '/opt/spark-1.6.0/assign2/stockprices/'
#directory2 = '/opt/spark-1.6.0/assign2/stockprices2/'
directory = '/opt/spark-1.6.0/assign2/modeltest/'
directory2 = '/opt/spark-1.6.0/assign2/modeltest2/'
files = os.listdir('%s' % directory2)
count = 0
figures = 1

# delete repeated lines

for f in files:
	lines_seen = set() # holds lines already seen
	outfile = open("%scleaned_%s" % (directory,f), 'w')
	for line in open('%s%s' % (directory2,f), 'r'):
	    if line not in lines_seen: # not a duplicate
		outfile.write(line)
		lines_seen.add(line)
	outfile.close()

# normalize share volume

for f in files:
	x = open('%scleaned_%s' % (directory,f), 'r')
	y = file("%s%s" % (directory,f), 'w+')
	print count,f
	countline = 0
	for line in x:
		#y.write(str(line.split(',',1)[0]) + ',')
		l = line.replace(',', ' ')
		a = l.split(' ')
		y.write(str(a[0]) + ',')	
		for i in a[0:-2]: y.write(i + ' ')
		if countline == 0: # take first line value as normalization reference
			v = float(l.split(' ')[-1])
			p = float(l.split(' ')[0])
		adj = math.log10(abs(float(l.split(' ')[-1]) - v) + 1)/p
		if float(l.split(' ')[-1]) > v: y.write(str(p + adj) + '\n')
		else: y.write(str(p - adj) + '\n')
		countline += 1
	x.close()
	y.close()
	os.remove('%scleaned_%s' % (directory,f))
	count += 1
