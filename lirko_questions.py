import os
from pyspark.sql.functions import col, count, desc, row_number, avg, explode, array_contains, floor, max as spark_max
from pyspark.sql.window import Window
from extract import (
    title_basics_df,
    title_ratings_df,
    title_crew_df,
    name_basics_df,
    title_principals_df
)

def l8_top_5_actors_per_decade():
    print("Executing Lirko Q8: Топ 5 акторів для кожного десятиліття")
    movies = title_basics_df.filter(col("titleType") == "movie").select("tconst", "startYear")
    actors_in_movies = title_principals_df.filter(col("category").isin("actor", "actress")) \
        .join(movies, "tconst") \
        .filter(col("startYear").isNotNull())
    
    actors_with_decade = actors_in_movies.withColumn("decade", (floor(col("startYear") / 10) * 10))
    
    actor_counts = actors_with_decade.groupBy("decade", "nconst").agg(count("tconst").alias("movie_count"))
    
    window_spec = Window.partitionBy("decade").orderBy(desc("movie_count"))
    top_actors = actor_counts.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 5)
    
    top_actors.join(name_basics_df, "nconst") \
        .select("decade", "primaryName", "movie_count", "rank") \
        .orderBy("decade", "rank") \
        .toPandas().to_csv("results/l8_top_5_actors_per_decade.csv", index=False)

def l9_actors_director_pairs():
    print("Executing Lirko Q9: Актори, які найчастіше грали у режисерів")
    directors = title_crew_df.withColumn("director", explode(col("directors"))) \
        .select("tconst", "director")
    actors = title_principals_df.filter(col("category").isin("actor", "actress")) \
        .select("tconst", "nconst")
    
    pairs = actors.join(directors, "tconst")
    pair_counts = pairs.groupBy("nconst", "director").agg(count("tconst").alias("collaboration_count"))
    
    top_pairs = pair_counts.orderBy(desc("collaboration_count")).limit(20)
    
    # Join with names for both actor and director
    top_pairs.join(name_basics_df.select(col("nconst").alias("nconst"), col("primaryName").alias("actor_name")), "nconst") \
        .join(name_basics_df.select(col("nconst").alias("director"), col("primaryName").alias("director_name")), "director") \
        .select("actor_name", "director_name", "collaboration_count") \
        .toPandas().to_csv("results/l9_actors_director_pairs.csv", index=False)

def l10_top_10_actors_by_rating():
    print("Executing Lirko Q10: Топ 10 акторів за середнім рейтингом")
    movies_ratings = title_basics_df.filter(col("titleType") == "movie") \
        .join(title_ratings_df, "tconst")
    
    actor_ratings = title_principals_df.filter(col("category").isin("actor", "actress")) \
        .join(movies_ratings, "tconst") \
        .groupBy("nconst").agg(
            avg("averageRating").alias("avg_rating"),
            count("tconst").alias("movie_count")
        ).filter(col("movie_count") >= 10)
    
    top_actors = actor_ratings.orderBy(desc("avg_rating")).limit(10)
    
    top_actors.join(name_basics_df, "nconst") \
        .select("primaryName", "avg_rating", "movie_count") \
        .toPandas().to_csv("results/l10_top_10_actors_by_rating.csv", index=False)

def l11_best_movie_for_top_100_actors():
    print("Executing Lirko Q11: Найкращий фільм для топ 100 популярних акторів")
    # Popularity defined by numVotes sum or just presence? 
    # Let's say top 100 actors by total numVotes across their movies.
    actor_votes = title_principals_df.filter(col("category").isin("actor", "actress")) \
        .join(title_ratings_df, "tconst") \
        .groupBy("nconst").agg(count("tconst").alias("movie_count"), avg("averageRating").alias("avg_r")) \
        .orderBy(desc("movie_count")).limit(100) # Simple metric for popular actors
    
    actor_movies = actor_votes.select("nconst").join(title_principals_df, "nconst") \
        .join(title_basics_df.filter(col("titleType") == "movie"), "tconst") \
        .join(title_ratings_df, "tconst")
    
    window_spec = Window.partitionBy("nconst").orderBy(desc("averageRating"), desc("numVotes"))
    best_movie = actor_movies.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") == 1)
    
    best_movie.join(name_basics_df, "nconst") \
        .select("primaryName", "primaryTitle", "averageRating") \
        .toPandas().to_csv("results/l11_best_movie_for_top_100_actors.csv", index=False)

def l12_actor_director_comparison():
    print("Executing Lirko Q12: Порівняння акторів-режисерів")
    # Get movies with their directors and actors
    directors = title_crew_df.withColumn("director", explode(col("directors"))) \
        .select("tconst", "director")
    actors = title_principals_df.filter(col("category").isin("actor", "actress")) \
        .select("tconst", "nconst")
    
    movie_info = directors.join(actors, "tconst") \
        .join(title_ratings_df, "tconst")
    
    comparison = movie_info.withColumn("is_actor_director", col("director") == col("nconst"))
    
    results = comparison.groupBy("is_actor_director").agg(avg("averageRating").alias("avg_rating"))
    
    results.toPandas().to_csv("results/l12_actor_director_comparison.csv", index=False)

def run_lirko_questions():
    l8_top_5_actors_per_decade()
    l9_actors_director_pairs()
    l10_top_10_actors_by_rating()
    l11_best_movie_for_top_100_actors()
    l12_actor_director_comparison()
