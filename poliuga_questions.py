import os
from pyspark.sql.functions import col, count, desc, row_number, avg, explode, size, array_sort, concat_ws, array_contains
from pyspark.sql.window import Window
from extract import (
    title_basics_df,
    title_ratings_df
)

def p13_most_popular_genre_per_year_2000_2020():
    print("Executing Poliuga Q13: Найпопулярніший жанр по роках (2000-2020)")
    movies = title_basics_df.filter(
        (col("titleType") == "movie") & 
        (col("startYear") >= 2000) & 
        (col("startYear") <= 2020) &
        (col("genres").isNotNull())
    )
    
    genres_per_year = movies.withColumn("genre", explode(col("genres"))) \
        .groupBy("startYear", "genre").agg(count("tconst").alias("movie_count"))
    
    window_spec = Window.partitionBy("startYear").orderBy(desc("movie_count"))
    top_genre = genres_per_year.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") == 1)
    
    top_genre.select("startYear", "genre", "movie_count") \
        .orderBy("startYear") \
        .toPandas().to_csv("results/p13_most_popular_genre_per_year_2000_2020.csv", index=False)

def p14_genre_runtime_growth():
    print("Executing Poliuga Q14: Жанри з найбільшим зростанням тривалості")
    # Compare 2000-2005 vs 2015-2020
    movies = title_basics_df.filter(
        (col("titleType") == "movie") & 
        (col("runtimeMinutes").isNotNull()) &
        (col("genres").isNotNull())
    )
    
    movies_exploded = movies.withColumn("genre", explode(col("genres")))
    
    avg_runtime = movies_exploded.groupBy("genre").agg(
        avg(when((col("startYear") >= 2000) & (col("startYear") <= 2005), col("runtimeMinutes"))).alias("avg_2000_2005"),
        avg(when((col("startYear") >= 2015) & (col("startYear") <= 2020), col("runtimeMinutes"))).alias("avg_2015_2020")
    ).filter(col("avg_2000_2005").isNotNull() & col("avg_2015_2020").isNotNull())
    
    growth = avg_runtime.withColumn("runtime_growth", col("avg_2015_2020") - col("avg_2000_2005")) \
        .orderBy(desc("runtime_growth"))
    
    growth.toPandas().to_csv("results/p14_genre_runtime_growth.csv", index=False)

def p15_genre_combinations_rating():
    print("Executing Poliuga Q15: Рейтинг комбінацій жанрів")
    # Combinations of 3 genres
    movies = title_basics_df.filter(
        (col("titleType") == "movie") & 
        (size(col("genres")) == 3)
    ).join(title_ratings_df, "tconst")
    
    # Sort genres to treat (A,B,C) the same as (C,B,A)
    comb_ratings = movies.withColumn("genre_comb", concat_ws(", ", array_sort(col("genres")))) \
        .groupBy("genre_comb").agg(
            avg("averageRating").alias("avg_rating"),
            count("tconst").alias("movie_count")
        ).filter(col("movie_count") >= 10) \
        .orderBy(desc("avg_rating"))
    
    comb_ratings.limit(20).toPandas().to_csv("results/p15_genre_combinations_rating.csv", index=False)

def p17_adult_movies_share_by_year():
    print("Executing Poliuga Q17: Частка фільмів для дорослих по роках")
    movies = title_basics_df.filter(col("titleType") == "movie") \
        .filter(col("startYear").isNotNull())
    
    shares = movies.groupBy("startYear").agg(
        avg(col("isAdult").cast("int")).alias("adult_share"),
        count("tconst").alias("total_count")
    ).orderBy("startYear")
    
    shares.toPandas().to_csv("results/p17_adult_movies_share_by_year.csv", index=False)

def p18_best_year_for_action():
    print("Executing Poliuga Q18: Рік з найвищим рейтингом Action")
    action_movies = title_basics_df.filter(
        (col("titleType") == "movie") & 
        (array_contains(col("genres"), "Action"))
    ).join(title_ratings_df, "tconst")
    
    best_year = action_movies.groupBy("startYear").agg(
        avg("averageRating").alias("avg_rating"),
        count("tconst").alias("movie_count")
    ).filter(col("movie_count") >= 50) \
        .orderBy(desc("avg_rating")).limit(1)
    
    best_year.toPandas().to_csv("results/p18_best_year_for_action.csv", index=False)

from pyspark.sql.functions import when

def run_poliuga_questions():
    p13_most_popular_genre_per_year_2000_2020()
    p14_genre_runtime_growth()
    p15_genre_combinations_rating()
    p17_adult_movies_share_by_year()
    p18_best_year_for_action()
