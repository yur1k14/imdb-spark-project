import os
from pyspark.sql.functions import col, avg, count, desc, row_number, array_contains
from pyspark.sql.window import Window
from extract import (
    title_basics_df,
    title_ratings_df,
    title_episode_df
)

def v2_top_5_series_by_avg_episode_rating():
    print("Executing Vorobets Q2: Топ 5 серіалів за середнім рейтингом епізодів")
    episodes_ratings = title_episode_df.join(title_ratings_df, "tconst")
    series_avg_rating = episodes_ratings.groupBy("parentTconst").agg(
        avg("averageRating").alias("avg_episode_rating"),
        count("tconst").alias("episode_count")
    ).filter(col("episode_count") >= 5)
    
    top_series = series_avg_rating.join(
        title_basics_df.withColumnRenamed("tconst", "parentTconst"), "parentTconst"
    ).orderBy(desc("avg_episode_rating")).limit(5)
    
    top_series.select("primaryTitle", "avg_episode_rating", "episode_count") \
        .toPandas().to_csv("results/v2_top_5_series_by_avg_episode_rating.csv", index=False)

def v3_top_20_worst_horror_movies():
    print("Executing Vorobets Q3: 20 найгірших фільмів жахів")
    horror_movies = title_basics_df.filter(
        (col("titleType") == "movie") & (array_contains(col("genres"), "Horror"))
    ).join(title_ratings_df, "tconst")
    
    worst_horror = horror_movies.filter(col("numVotes") >= 1000) \
        .orderBy("averageRating").limit(20)
    
    worst_horror.select("primaryTitle", "averageRating", "numVotes") \
        .toPandas().to_csv("results/v3_top_20_worst_horror_movies.csv", index=False)

def v4_top_3_movies_per_year():
    print("Executing Vorobets Q4: Топ 3 найкращі фільми для кожного року")
    movies = title_basics_df.filter(
        (col("titleType") == "movie") & (col("startYear").isNotNull())
    ).join(title_ratings_df.filter(col("numVotes") >= 10000), "tconst")
    
    window_spec = Window.partitionBy("startYear").orderBy(desc("averageRating"), desc("numVotes"))
    top_per_year = movies.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 3)
    
    top_per_year.select("startYear", "primaryTitle", "averageRating", "numVotes", "rank") \
        .toPandas().to_csv("results/v4_top_3_movies_per_year.csv", index=False)

def v5_top_10_popular_documentaries():
    if os.path.exists("results/v5_top_10_popular_documentaries.csv"):
        print("Skipping Vorobets Q5: File already exists.")
        return
    print("Executing Vorobets Q5: Топ 10 документальних фільмів за кількістю голосів")
    docs = title_basics_df.filter(
        (col("titleType") == "movie") & (array_contains(col("genres"), "Documentary"))
    ).join(title_ratings_df, "tconst")
    
    top_docs = docs.orderBy(desc("numVotes")).limit(10)
    
    top_docs.select("primaryTitle", "numVotes", "averageRating") \
        .toPandas().to_csv("results/v5_top_10_popular_documentaries.csv", index=False)

def v6_avg_rating_per_year_2000_2020():
    print("Executing Vorobets Q6: Середній рейтинг фільмів по роках (2000-2020)")
    movies = title_basics_df.filter(
        (col("titleType") == "movie") & 
        (col("startYear") >= 2000) & 
        (col("startYear") <= 2020)
    ).join(title_ratings_df, "tconst")
    
    avg_per_year = movies.groupBy("startYear").agg(avg("averageRating").alias("avg_rating")) \
        .orderBy("startYear")
    
    avg_per_year.toPandas().to_csv("results/v6_avg_rating_per_year_2000_2020.csv", index=False)

def run_vorobets_questions():
    v2_top_5_series_by_avg_episode_rating()
    v3_top_20_worst_horror_movies()
    v4_top_3_movies_per_year()
    v5_top_10_popular_documentaries()
    v6_avg_rating_per_year_2000_2020()
