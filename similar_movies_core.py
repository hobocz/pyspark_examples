import sys
from pyspark import SparkConf, SparkContext
from math import sqrt
from datetime import datetime


def loadMovieNames():
    movieNames = {}
    with open("./movie_data/u.item",  encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames


def makePairs(userRatings):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))


def filterDupes(userRatings):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2


def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    # generate pairs scores
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1
    # set numerator, denominator
    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)
    # generate score
    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))
    return (score, numPairs)


def inputDataParser(data):
    tokens = data.split("\t")
    return (int(tokens[0]), (int(tokens[1]), int(tokens[2])))


# Start simple execution timer
start_time = datetime.now()

# Create the SparkContext
conf = SparkConf().setMaster("local[*]").setAppName("SimilarMovies")
sc = SparkContext(conf = conf)

# Load the movie names into a dictionary
# Note: This is a small enough sample dataset to be a reasonable choice
movieNameDict = loadMovieNames()

# Load the movie data
inputData = sc.textFile("./movie_data/u.data")

# Map ratings to key-value pairs: (userID, (movieID, rating))
ratings = inputData.map(inputDataParser)

# Perform a self-join to find every pair of movies rated by the same user.
joinedRatings = ratings.join(ratings)
# At this point the RDD should look like: (userID, ((movieID, rating), (movieID, rating)))

moviePairSimilarities = (joinedRatings
    # Filter out same movie and duplicate pairs
    .filter(filterDupes)
    # Reorganize by movie & rating pairs
    .map(makePairs)
    # Group by movie pairs
    .groupByKey()
    # Compute the scores
    .mapValues(computeCosineSimilarity)
    # Sort by score
    .sortByKey()
)

# Set thresholds for min score and min num pairs ('co-occurrence' or 'strength')
SCORE = 0.97
CO_OCCURRENCE = 50.0

if (len(sys.argv) > 1):
    movieID = int(sys.argv[1])
else:
    movieID = 132 # Default to "Wizard of Oz"

# Filter for movies matching the id and meeting above thresholds
filteredResults = moviePairSimilarities.filter(lambda pair:
    (pair[0][0] == movieID or pair[0][1] == movieID)
    and pair[1][0] > SCORE and pair[1][1] > CO_OCCURRENCE)

# Sort by quality score & get top 10
results = (filteredResults
    .map(lambda pair: (pair[1], pair[0]))
    .sortByKey(ascending = False)
    .take(10)
)

print("Top 10 similar movies for " + movieNameDict[movieID])
for result in results:
    (sim, pair) = result
    # Pick the correct similar movie out of the pair
    similarMovieID = pair[0]
    if (similarMovieID == movieID):
        similarMovieID = pair[1]
    name = movieNameDict[similarMovieID]
    score = round(sim[0], 4)
    strength = sim[1]
    print(f"score: {score}\tstrength: {strength}\t{name}")

end_time = datetime.now()
time_diff = (end_time - start_time).total_seconds()
print("Approx execution time:", time_diff, "secs")