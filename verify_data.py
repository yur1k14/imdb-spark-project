from extract import name_basics_df, title_basics_df, title_ratings_df, title_akas_df, title_crew_df, title_episode_df, \
    title_principals_df
from pyspark.sql.functions import col, sum

name_basics_df.show(5)
title_akas_df.show(5)
title_basics_df.show(5)
title_crew_df.show(5)
title_episode_df.show(5)
title_principals_df.show(5)
title_ratings_df.show(5)

name_basics_df.printSchema()
title_akas_df.printSchema()
title_basics_df.printSchema()
title_crew_df.printSchema()
title_episode_df.printSchema()
title_principals_df.printSchema()
title_ratings_df.printSchema()

print("name_basics:", name_basics_df.count())
print("title_akas:", title_akas_df.count())
print("title_basics:", title_basics_df.count())
print("title_crew:", title_crew_df.count())
print("title_episode:", title_episode_df.count())
print("title_principals:", title_principals_df.count())
print("title_ratings:", title_ratings_df.count())

print(title_basics_df.select("tconst").distinct().count())
print(title_ratings_df.select("tconst").distinct().count())
print(name_basics_df.select("nconst").distinct().count())

title_basics_df.select("titleType").distinct().show()
title_principals_df.select("category").distinct().show()

title_basics_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in title_basics_df.columns
]).show()

