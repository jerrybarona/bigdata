import yahoo_finance
from yahoo_finance import Share
import time, math, datetime, pytz
from pytz import timezone
from pyspark import SparkContext
import requests
import BeautifulSoup
import os.path
import sys

companies = ['BAC', 'C', 'IBM', 'AAPL', 'GE', 'T', 'MCD', 'NKE', 'TWTR', 'TSLA']
directory = 'assign2/paststockprices/'

if not len(sys.argv) == 4:
        print "ERROR: invalid number of command line arguments"
        print "SYNTAX: python assign2/pastgetstock.py <TEST_LABEL> <START_DATE> <END_DATE>"
        print "\t<TEST_LABEL>  : Tag to be assigned to output file name"
        print "\t<START_DATE>, <END_DATE>  : Ex.: 2016-02-21"
        sys.exit()

else:
	test = str(sys.argv[1])
	start_date = str(sys.argv[2])
	end_date = str(sys.argv[3])

for co in companies:
	print "Opening file: %s%s_past%s.txt" % (directory, co.lower(), test)
	companyfile = file('%s%s_past%s.txt' % (directory, co.lower(), test),'a+')
	try:
		yahoostock = Share(co)
		yahoostock.refresh()
		historical = yahoostock.get_historical(start_date, end_date)
		for day in historical:
			companyfile.write(day['Adj_Close'] + "," + day['Close'] + " " + day['High'] + " " + day['Low'] + " " + day['Open'] + " " + day['Volume'] + '\n') # open price
	except:
		print "%s did not connect to server or check command line input" % co	
