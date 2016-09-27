import yahoo_finance
from yahoo_finance import Share
import time, math, datetime, pytz
from pytz import timezone
from pyspark import SparkContext
import requests
import os
import BeautifulSoup
import os.path
import sys
import time

urlnasdaq = "http://www.nasdaq.com/symbol/"
ah = '/after-hours'
pm = '/premarket'
companies = ['BAC', 'C', 'IBM', 'AAPL', 'GE', 'T', 'MCD', 'NKE', 'TWTR', 'TSLA']
#directory = '/opt/spark-1.6.0/assign2/stockprices2/'
directory = '/opt/spark-1.6.0/assign2/modeltest2/'
sentimentdirectory = '/opt/spark-1.6.0/assign2/sentiments/'
maxcount = 1
if not len(sys.argv) == 3:
        print "ERROR: invalid number of command line arguments"
        print "SYNTAX: python getstock.py <NUMBER_OF_TWEETS> <TEST_LABEL>"
        print "\t<NUMBER_OF_TWEETS>  : Number of tweets used in the sentiment analysis"
        print "\t<TEST_LABEL>  : Tag to be assigned to output file name"
        sys.exit()

else:
	test = str(sys.argv[2])
	tweets = str(sys.argv[1])
count = 0
#os.system('python /opt/alchemyapi-recipes-twitter/recipe.py %s %s' % (tweets, test))
while (count < maxcount):
	for co in companies:
		print "Opening file: %s%s%s.txt" % (directory, co.lower(), test)
		companyfile = file('%s%s%s.txt' % (directory, co.lower(), test),'a+')
		try:
			yahoostock = Share(co)
			yahoostock.refresh()
			companyfile.write(yahoostock.get_price() + "," + yahoostock.get_prev_close() + ' ' + yahoostock.get_open() + ' ' + yahoostock.get_days_high() + ' ' + yahoostock.get_days_low() + ' ' + yahoostock.get_50day_moving_avg() + ' ' + yahoostock.get_200day_moving_avg() + ' ')
			source_code = requests.get("%s%s%s" % (urlnasdaq,co,ah))
			plain_text = source_code.text
			soup = BeautifulSoup.BeautifulSoup(plain_text)
			s = soup.find('div',{'id':'qwidget_lastsale', 'class':'qwidget-dollar'}).string
			if not s.replace('$','') == 'N/A':
				companyfile.write(s.replace('$','') + ' ')
			else:
				companyfile.write(yahoostock.get_prev_close() + ' ')
				
			source_code = requests.get("%s%s%s" % (urlnasdaq,co,pm))
			plain_text = source_code.text
			soup = BeautifulSoup.BeautifulSoup(plain_text)
			s = soup.find('div',{'id':'qwidget_lastsale', 'class':'qwidget-dollar'}).string
			if not s.replace('$','') == 'N/A':
				companyfile.write(s.replace('$','') + ' ')
			else:
				companyfile.write(yahoostock.get_open() + ' ')
			companyfile.write(yahoostock.get_volume() + '\n')
	#		sentimentfile = file('%stest%s.txt' % (sentimentdirectory, test),'r')
	#		for line in sentimentfile:
	#			if '%s,' % co in line:
	#				print line
	#				companyfile.write(' ' + line.split(',',1)[1])
			companyfile.close()
	#		sentimentfile.close()
			
		except:
			print "%s did not connect to server" % co
	count += 1
	if count >= maxcount: break
	print count
	time.sleep(120)			
