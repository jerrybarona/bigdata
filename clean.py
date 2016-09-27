import glob, os, sys
directory = '/opt/spark-1.6.0/assign2/stockprices/'
directory2 = '/opt/spark-1.6.0/assign2/stockprices2/'
files = os.listdir('%s' % directory)
print files
count = 0
for f in files:
	x = open('%s%s' % (directory,f), 'r')
	y = file("%s%s" % (directory2,f), 'w+')
	print count,f
	for line in x:
		y.write(str(line.split(',',1)[0]) + ',')
		a = line.split(',',1)[1]
		for i in a.split(' ')[0:5]: y.write(i + ' ')
		b = a.split(' ')[5]
		#print b
		m = b.split('.',2)[1]
		#print m[-2:]
		if abs(float(m[-2:]) - float(line.split(',',1)[0])) < abs(float(m[-3:]) - float(line.split(',',1)[0])):
			y.write(str(b.split('.',1)[0]) + '.' +  m[:-2] + ' ' + m[-2:] + '.' + str(b.split('.',2)[2]))
		else:
			y.write(str(b.split('.',1)[0]) + '.' +  m[:-3] + ' ' + m[-3:] + '.' + str(b.split('.',2)[2]))
			
		#print b.split('.',1)[0]
		for i in a.split(' ')[6:]: y.write(' ' + i)		
	count += 1
#	if count > 0: break
				
