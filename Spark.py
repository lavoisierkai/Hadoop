from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
	movieNames = {}
	with open("ml-100k/u.item") as f:
		for linw in f:
			fields = line.split('|')
			movieNames[int(field[0])] = fields[1]
	return movieNames

def parseInput(line):
	fields = line.split()
	return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
	spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

	movieNames = loadMovieNames()

	lines = spark.sparkContext.textFile("hdfs:///usr/maria_dev/ml-100k/u.data")

	moives = line.map(parseInput)

	movieDataset = spark.createDataFrame(moives)

	averageRatings = movieDataset.groupBy("movieID").avg("rating")

	counts = movieDataset.groupBy("movieID").count()

	averagesAndCounts = counts.join(averageRatings, "movieID")

	topTen = averagesAndCounts.orderBy("avg(rating)").take(10)

	for moives in topTen:
		print(movieNames[moives[0], moives[1], moives[2]])

	spark.stop()