from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD, LinearRegressionModel
from pyspark import SparkContext
import glob, os

#directory = 'assign2/paststockprices/'
directory = '/opt/spark-1.6.0/assign2/stockprices/'
directoryt = '/opt/spark-1.6.0/assign2/modeltest/'
mstep = 0.000075 # initial 
stepfactor = 2.5
sc = SparkContext()
mstep_t = [0,0,0]
MSE_t = [0,0,0]
# Load and parse the data

def parsePoint(line):
    values = [float(x) for x in line.replace(',', ' ').split(' ')]
    return LabeledPoint(values[0], values[1:])

allfiles = os.listdir('%s' % directory)
alltestfiles = os.listdir('%s' % directoryt)

for f in allfiles:
	data = sc.textFile('%s%s' % (directory, f))
	parsedData = data.map(parsePoint)
	# Build the model
	mstep_t[0] = mstep
	model0 = LinearRegressionWithSGD.train(parsedData, iterations = 500, step = mstep_t[0], intercept = True)
	model = model0
	# Evaluate the model on training data
	valuesAndPreds0 = parsedData.map(lambda p: (p.label, model0.predict(p.features)))
	MSE_t[0] = valuesAndPreds0.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds0.count()
	MSE = MSE_t[0]	
	mstep_t[1] = mstep_t[0]*stepfactor
	model1 = LinearRegressionWithSGD.train(parsedData, iterations = 500, step = mstep_t[1], intercept = True)
	valuesAndPreds1 = parsedData.map(lambda p: (p.label, model1.predict(p.features)))
	MSE_t[1] = valuesAndPreds1.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds1.count()
	
	if MSE_t[1] < MSE_t[0]:
		MSE_t[2] = MSE_t[0]
		mstep_t[0] = mstep_t[1]
		MSE_t[0] = MSE_t[1]
		for i in range(1,9):
			mstep_t[1] = mstep_t[0]*stepfactor*(11 - i)/10
			model1 = LinearRegressionWithSGD.train(parsedData, iterations = 500, step = mstep_t[1], intercept = True)
#			if i == 6: model = model1	
			valuesAndPreds1 = parsedData.map(lambda p: (p.label, model1.predict(p.features)))
			MSE_t[1] = valuesAndPreds1.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds1.count()
			if MSE_t[1] < MSE_t[0]:
				MSE_t[2] = MSE_t[0]
				mstep_t[0] = mstep_t[1]
				MSE_t[0] = MSE_t[1]
				model = model1	
			else:
				if MSE_t[1] > MSE_t[2]:
					mstep_t[1] = mstep_t[1] / (stepfactor ** 2)
					MSE_t[1] = MSE_t[2]
#				print "Increased step %d times\n" % i
				break
	
	else:
		MSE_t[2] = MSE_t[1]
		for i in range(0,9):
			mstep_t[1] = mstep_t[0]/(stepfactor*(9 + i)/10)
			model1 = LinearRegressionWithSGD.train(parsedData, iterations = 500, step = mstep_t[1], intercept = True)
			valuesAndPreds1 = parsedData.map(lambda p: (p.label, model1.predict(p.features)))
			MSE_t[1] = valuesAndPreds1.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds1.count()
			if MSE_t[1] < MSE_t[0]:
				MSE_t[2] = MSE_t[0]
				mstep_t[0] = mstep_t[1]
				MSE_t[0] = MSE_t[1]
			else:
				if MSE_t[1] > MSE_t[2]:
					mstep_t[1] = mstep_t[1] / (stepfactor ** 2)
					MSE_t[1] = MSE_t[2]
#				print "Decreased step %d times\n" % i
				break
	
	for i in range(0,4):
		mstep_t[2] = mstep_t[1]
		MSE_t[2] = MSE_t[1]
		mstep_t[1] = abs(mstep_t[0] - mstep_t[1])/2
		model1 = LinearRegressionWithSGD.train(parsedData, iterations = 500, step = mstep_t[1], intercept = True)
		valuesAndPreds1 = parsedData.map(lambda p: (p.label, model1.predict(p.features)))
		MSE_t[1] = valuesAndPreds1.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds1.count()
		if MSE_t[1] < MSE_t[0]:
			MSE_t[0], MSE_t[1] = MSE_t[1], MSE_t[0]
			mstep_t[0], mstep_t[1] = mstep_t[1], mstep_t[0]
		else:
#			print "Halved step %d times\n" % i
			break	

	print str(f)
	print model1#, model1.weights[0]
	print("Original Mean Squared Error = " + str(MSE) + ", Step: " + str(mstep))
	print("Optimized Mean Squared Error = " + str(MSE_t[0]) + ", Step: " + str(mstep_t[0]))
	data = sc.textFile('%s%s' % (directoryt, f))
	parsedData = data.map(parsePoint)
#	mstep_t[0] = mstep
#	model1 = LinearRegressionWithSGD.train(parsedData, iterations = 500, step = mstep_t[0], intercept = True)
	if not model1.weights[0] == 0:
		valuesAndPreds0 = parsedData.map(lambda p: (p.label, model1.predict(p.features)))
	else:
		valuesAndPreds0 = parsedData.map(lambda p: (p.label, model.predict(p.features)))
	print "Value (test, predicted): ", valuesAndPreds0.collect(),'\n'
