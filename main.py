from operations import (
    get_high_rated_titles,
    get_top_genres_by_rating,
    get_most_frequent_actors_in_top_movies,
    get_runtime_trends_by_decade
)
from extract import (
    title_basics_df,
    title_ratings_df,
    title_principals_df,
    name_basics_df
)

print("Топ 10 фільмів з високим рейтингом (мінімум 10 000 голосів)")
top_movies = get_high_rated_titles(title_basics_df, title_ratings_df)
top_movies.select("primaryTitle", "startYear", "averageRating", "numVotes").show(10, truncate=False)

print("Топ жанрів за середнім рейтингом")
top_genres = get_top_genres_by_rating(title_basics_df, title_ratings_df)
top_genres.show(10, truncate=False)

print("Актори з найбільшою кількістю ролей у фільмах з рейтингом 8.0+")
top_actors = get_most_frequent_actors_in_top_movies(title_principals_df, title_ratings_df, name_basics_df)
top_actors.show(10, truncate=False)

print("Еволюція тривалості фільмів по десятиліттях")
runtime_trends = get_runtime_trends_by_decade(title_basics_df)
runtime_trends.show(15)
