### PySpark scripts demonstrating `Spark Core` and `Spark SQL` functionality

[Spark 3.5.3, Python 3.12.3, OpenJDK 17.0.12]

Basic testing was performed in Ubuntu linux (24.04).

These scripts give movie recommendations based on similar ratings from users.
This is a very small sample dataset stored uncompressed in the `movie_data`
folder (attribution: [grouplens.org](https://grouplens.org/datasets/movielens/100k/)) meant for development and testing.

The similarity scores are calculated using cosine similarity. The file
`similar_movies_core.py` demonstrates the process using **RDDs** and 
`similar_movies_sql.py` uses **DataFrames**. The SparkSQL example is much faster
but the Spark Core example is included for demonstration and to act as
a reference.

Note 1: Both scripts are meant to run on a _local_ development/test environment.
When run on a cluster, some adjustments would need to be made as well as some possible 
additions (eg: partitions, persistence, etc).

Note 2: Rigorous testing is still required as there are certainly
some environments, parameters, etc, that were not accommodated and may
uncover issues.
