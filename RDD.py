from pyspark import Sparkconf, SparkContext

def loadMovieNames():
	movieName = {};
	with open("ml-100l/u.item") as f:
		for line in f:
			fields = line.split('|')
			movieName[int(fields[0])] = fields[1]
	return movieName

def parseInput(line):
	fiels = line.split()
	return (int(fields[1]), (float(fields[2], 1.0)))

if __name__ = "__main__":
	conf = Sparkconf().setAppName("WorstMovies")
	sc = SparkContext(conf = conf)

	movieName = loadMovieNames()

	lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

	movieRatings = lines.map(parseInput)

	ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: (movie1[0] + movie2[0]))

	averageRatings = ratingTotalsAndCount.mapvalues(lambda totalAndCount : totalAndCount[0]/totalAndCount[1] )

	sortedMovies = averageRatings.sortBy(lambda x: x[1])

	results = sortedMovies.take(10)

	for result in results:
		print(movieNames[result[0]], result[1])