"""
    Job finds the most demanded actors. The actor is demanded if has played in top movies more than one time.
"""

from spark_tasks import task4_topActors as task4
from spark_tasks.manageDFs import create_df, write_in_single_csv_file

from spark_tasks.task1_top_movies.top100movies import get_movies_with_rating

actorsDF = create_df("..\\..\\Datasets\\name.basics.tsv.gz")

actorStaffInMovies = create_df("..\\..\\Datasets\\title.principals.tsv.gz")

moviesWithRatingAndGenres = get_movies_with_rating()

topMoviesWithRating = moviesWithRatingAndGenres.filter(moviesWithRatingAndGenres.numVotes >= 100000) \
    .orderBy(task4.col('averageRating').desc())\
    .drop("genres")

actorStaffInMovies = actorStaffInMovies.filter(((actorStaffInMovies.category == "actor")
                                               | (actorStaffInMovies.category == "actress"))
                                               & (actorStaffInMovies.characters != "\\N"))\
    .join(topMoviesWithRating, actorStaffInMovies.tconst == topMoviesWithRating.tconst)\
    .select(topMoviesWithRating.tconst,
            actorStaffInMovies.nconst)

liveActorsDF = actorsDF.filter((actorsDF.deathYear == '\\N')
                               & (actorsDF.knownForTitles != "\\N"))

liveActorsInTopMovies = actorStaffInMovies.join(liveActorsDF, liveActorsDF.nconst == actorStaffInMovies.nconst)\
    .select(liveActorsDF.primaryName,
            actorStaffInMovies.tconst)

w = task4.Window.partitionBy("primaryName").orderBy("primaryName")

topActors = liveActorsInTopMovies.withColumn("hot_top_actors", task4.row_number().over(w))\
    .filter(task4.col("hot_top_actors") > 1)

topActors = topActors.drop_duplicates(["primaryName"]).orderBy("primaryName").drop("hot_top_actors", "tconst")

write_in_single_csv_file(topActors, "..\\..\\Output\\topActors")

task4.spark.stop()
