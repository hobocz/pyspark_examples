from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import sys
from datetime import datetime

def computeCosineSimilarity(data):
    # generate pairs scores
    pairScores = (data
        .withColumn("xx", F.col("rating1") * F.col("rating1"))
        .withColumn("yy", F.col("rating2") * F.col("rating2"))
        .withColumn("xy", F.col("rating1") * F.col("rating2"))
    )
    # generate numerator, denominator and numPairs columns
    numerDenomNum = (pairScores
        .groupBy("movie1", "movie2")
        .agg(F.sum(F.col("xy")).alias("numerator"),
            (F.sqrt(F.sum(F.col("xx"))) * F.sqrt(F.sum(F.col("yy")))).alias("denominator"),
            F.count(F.col("xy")).alias("numPairs")
        )
    )
    # generate score and remove the extra intermediate columns
    result = (numerDenomNum
        .withColumn("score",
            F.when(F.col("denominator") != 0, F.col("numerator") / F.col("denominator"))
            .otherwise(0)
        )
        .select("movie1", "movie2", "score", "numPairs")
    )
    return result


# Get movie name by id 
def getMovieName(movieNames, movieId):
    result = movieNames.filter(F.col("movieID") == movieId).select("movieTitle").collect()[0]
    return result[0]


# Start simple execution timer
start_time = datetime.now()

# Create the SparkSession
spark = SparkSession.builder.appName("SimilarMovies").master("local[*]").getOrCreate()

# Create the schema for he movie names dataframe
movieNamesSchema = T.StructType([T.StructField("movieID", T.IntegerType(), True),
                                 T.StructField("movieTitle", T.StringType(), True)]
)

# Create the schema for he movie data dataframe
moviesSchema = T.StructType([T.StructField("userID", T.IntegerType(), True),
                             T.StructField("movieID", T.IntegerType(), True),
                             T.StructField("rating", T.IntegerType(), True),
                             T.StructField("timestamp", T.LongType(), True)]
)

# Read movie names data
movieNames = (spark
    .read
    .option("sep", "|")
    .option("charset", "ISO-8859-1")
    .schema(movieNamesSchema)
    .csv("./movie_data/u.item")
)

# Read movie data
movies = (spark
    .read
    .option("sep", "\t")
    .schema(moviesSchema)
    .csv("./movie_data/u.data")
)

# Eliminate the timestamp column
ratings = movies.select("userId", "movieId", "rating")

# Perform a self-join to find every pair of movies rated by the same user.
# Select movie pairs and rating pairs
moviePairs = (ratings
    .alias("ratings1")
    .join(ratings.alias("ratings2"),
        (F.col("ratings1.userId") == F.col("ratings2.userId")) 
        & (F.col("ratings1.movieId") < F.col("ratings2.movieId")) # <-- eliminate same movie and dupes
    )
    .select(F.col("ratings1.movieId").alias("movie1"),
        F.col("ratings2.movieId").alias("movie2"),
        F.col("ratings1.rating").alias("rating1"),
        F.col("ratings2.rating").alias("rating2")
    )
)

moviePairSimilarities = computeCosineSimilarity(moviePairs)

# Set thresholds for min score and min num pairs ('co-occurrence' or 'strength')
SCORE = 0.97
CO_OCCURRENCE = 50.0

if (len(sys.argv) > 1):
    movieID = int(sys.argv[1])
else:
    movieID = 132 # Default to "Wizard of Oz"

# Filter for movies matching the id and meeting above thresholds
filteredResults = moviePairSimilarities.filter(
    ((F.col("movie1") == movieID) | (F.col("movie2") == movieID))
    & (F.col("score") > SCORE) & (F.col("numPairs") > CO_OCCURRENCE)
)

# Sort by quality score & get top 10
results = filteredResults.sort(F.col("score").desc()).take(10)

print ("Top 10 similar movies for " + getMovieName(movieNames, movieID))
for result in results:
    # Pick the correct similar movie out of the pair
    similarMovieID = result.movie1
    if (similarMovieID == movieID):
        similarMovieID = result.movie2
    name = getMovieName(movieNames, similarMovieID)
    score = str(round(result["score"], 4))
    strength = str(result["numPairs"])
    print(f"score: {score}\tstrength: {strength}\t{name}")
        
end_time = datetime.now()
time_diff = (end_time - start_time).total_seconds()
print("Approx execution time:", time_diff, "secs")