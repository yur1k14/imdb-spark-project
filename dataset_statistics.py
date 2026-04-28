from extract import title_basics_df, title_ratings_df, name_basics_df, spark
from pyspark.sql.functions import col, count, isnull, when

def get_general_statistics():
    print("=== GENERAL STATISTICAL INFORMATION ===")

    # 1. Total row counts
    print("\n--- Total Row Counts ---")
    print(f"Titles (title.basics): {title_basics_df.count():,}")
    print(f"Ratings (title.ratings): {title_ratings_df.count():,}")
    print(f"Names/People (name.basics): {name_basics_df.count():,}")

    # 2. Distribution of Title Types
    print("\n--- Distribution of Title Types ---")
    title_basics_df.groupBy("titleType").count().orderBy(col("count").desc()).show(truncate=False)

    # 3. Missing values in key columns for title basics
    print("\n--- Missing Values in Key Columns (title_basics) ---")
    title_basics_df.select([
        count(when(isnull(c), c)).alias(c) for c in ["startYear", "runtimeMinutes", "genres"]
    ]).show()


def get_numerical_statistics():
    print("\n=== STATISTICS ON NUMERICAL FEATURES ===")

    # 1. Title Basics (Release Year & Runtime)
    print("\n--- Title Basics (startYear, endYear, runtimeMinutes) ---")
    # .summary() provides count, mean, stddev, min, 25%, 50%, 75%, and max
    title_basics_df.select("startYear", "endYear", "runtimeMinutes").summary().show()

    # 2. Ratings Data (Average Rating & Number of Votes)
    print("\n--- Ratings Data (averageRating, numVotes) ---")
    title_ratings_df.select("averageRating", "numVotes").summary().show()

    # 3. People Data (Birth & Death Years)
    print("\n--- People Data (birthYear, deathYear) ---")
    name_basics_df.select("birthYear", "deathYear").summary().show()


if __name__ == "__main__":
    # Set log level to WARN to keep console output clean from Spark info logs
    spark.sparkContext.setLogLevel("WARN")

    get_general_statistics()
    get_numerical_statistics()
