import os
from pyspark.sql.functions import col, count, desc, row_number, avg, max as spark_max, min as spark_min, explode, array_contains, collect_list, lag
from pyspark.sql.window import Window
from extract import (
    title_basics_df,
    title_ratings_df,
    title_episode_df
)

def s20_top_3_episodes_for_top_10_series():
    print("Executing Sydor Q20: Топ 3 епізоди для топ 10 популярних серіалів")
    top_10_series = title_basics_df.filter(col("titleType").isin("tvSeries", "tvMiniSeries")) \
        .join(title_ratings_df, "tconst") \
        .orderBy(desc("numVotes")).limit(10) \
        .select(col("tconst").alias("parentTconst"), "primaryTitle")
    
    episodes = title_episode_df.join(top_10_series, "parentTconst") \
        .join(title_ratings_df, "tconst") \
        .join(title_basics_df.select(col("tconst").alias("tconst"), col("primaryTitle").alias("episodeTitle")), "tconst")
    
    window_spec = Window.partitionBy("parentTconst").orderBy(desc("averageRating"), desc("numVotes"))
    top_episodes = episodes.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 3)
    
    top_episodes.select("primaryTitle", "episodeTitle", "averageRating", "rank") \
        .toPandas().to_csv("results/s20_top_3_episodes_for_top_10_series.csv", index=False)

def s21_series_with_growing_ratings():
    print("Executing Sydor Q21: Серіали з ростучим рейтингом сезонів")
    season_avg = title_episode_df.join(title_ratings_df, "tconst") \
        .groupBy("parentTconst", "seasonNumber").agg(avg("averageRating").alias("avg_season_rating")) \
        .filter(col("seasonNumber").isNotNull())
    
    window_spec = Window.partitionBy("parentTconst").orderBy("seasonNumber")
    growing = season_avg.withColumn("prev_rating", lag("avg_season_rating").over(window_spec)) \
        .withColumn("is_growing", (col("prev_rating").isNull()) | (col("avg_season_rating") > col("prev_rating")))
    
    series_status = growing.groupBy("parentTconst").agg(
        spark_min(col("is_growing").cast("int")).alias("all_growing"),
        count("seasonNumber").alias("season_count")
    ).filter((col("all_growing") == 1) & (col("season_count") >= 3))
    
    series_status.join(title_basics_df.withColumnRenamed("tconst", "parentTconst"), "parentTconst") \
        .select("primaryTitle", "season_count") \
        .orderBy(desc("season_count")).limit(20) \
        .toPandas().to_csv("results/s21_series_with_growing_ratings.csv", index=False)

def s22_avg_episodes_per_season_by_genre():
    print("Executing Sydor Q22: Середня кількість епізодів у сезоні за жанром")
    series_genres = title_basics_df.filter(col("titleType").isin("tvSeries", "tvMiniSeries")) \
        .select("tconst", "genres") \
        .withColumn("genre", explode(col("genres")))
    
    season_counts = title_episode_df.groupBy("parentTconst", "seasonNumber") \
        .agg(count("tconst").alias("episode_count")) \
        .filter(col("seasonNumber").isNotNull())
    
    genre_stats = season_counts.join(series_genres.withColumnRenamed("tconst", "parentTconst"), "parentTconst") \
        .groupBy("genre").agg(avg("episode_count").alias("avg_episodes_per_season")) \
        .orderBy(desc("avg_episodes_per_season"))
    
    genre_stats.toPandas().to_csv("results/s22_avg_episodes_per_season_by_genre.csv", index=False)

def s23_series_with_most_seasons():
    print("Executing Sydor Q23: 10 серіалів з найбільшою кількістю сезонів")
    most_seasons = title_episode_df.groupBy("parentTconst") \
        .agg(spark_max(col("seasonNumber").cast("int")).alias("season_count")) \
        .orderBy(desc("season_count")).limit(10)
    
    most_seasons.join(title_basics_df.withColumnRenamed("tconst", "parentTconst"), "parentTconst") \
        .select("primaryTitle", "season_count") \
        .toPandas().to_csv("results/s23_series_with_most_seasons.csv", index=False)

def s24_best_episode_per_season():
    print("Executing Sydor Q24: Найкращий епізод у кожному сезоні")
    episodes = title_episode_df.join(title_ratings_df, "tconst") \
        .join(title_basics_df.select(col("tconst").alias("tconst"), "primaryTitle"), "tconst") \
        .filter(col("seasonNumber").isNotNull())
    
    window_spec = Window.partitionBy("parentTconst", "seasonNumber").orderBy(desc("averageRating"), desc("numVotes"))
    best_ep = episodes.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") == 1)
    
    best_ep.join(title_basics_df.select(col("tconst").alias("parentTconst"), col("primaryTitle").alias("seriesTitle")), "parentTconst") \
        .select("seriesTitle", "seasonNumber", "primaryTitle", "averageRating") \
        .limit(100) \
        .toPandas().to_csv("results/s24_best_episode_per_season.csv", index=False)

def run_sydor_questions():
    s20_top_3_episodes_for_top_10_series()
    s21_series_with_growing_ratings()
    s22_avg_episodes_per_season_by_genre()
    s23_series_with_most_seasons()
    s24_best_episode_per_season()
