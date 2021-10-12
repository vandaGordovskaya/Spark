"""
    Job finds top-5 movies by rating for each existed director.
"""

from spark_tasks import task5_top5_movies_for_director as task5
from spark_tasks.manageDFs import create_df, write_in_single_csv_file
from spark_tasks.task1_top_movies.top100movies import get_movies_with_rating

crewDF = create_df("..\\..\\Datasets\\title.crew.tsv.gz")

namesDF = create_df("..\\..\\Datasets\\name.basics.tsv.gz")

moviesWithRatingAndGenres = get_movies_with_rating()

directors = crewDF.drop("writers")
director = directors\
    .select("tconst", task5.explode(task5.split(task5.col("directors"), ",")).alias("director"))\
    .drop("directors")

namedDirector = director.join(namesDF, director.director == namesDF.nconst)\
    .select(director.tconst, namesDF.primaryName)

directorAndMovies = namedDirector\
    .join(moviesWithRatingAndGenres, namedDirector.tconst == moviesWithRatingAndGenres.tconst, 'inner')\
    .select(namedDirector.primaryName,
            moviesWithRatingAndGenres.primaryTitle,
            moviesWithRatingAndGenres.startYear,
            moviesWithRatingAndGenres.averageRating,
            moviesWithRatingAndGenres.numVotes)

w = task5.Window.partitionBy("primaryName").orderBy(task5.col("averageRating").desc())
directorsWithTop5movies = directorAndMovies\
    .withColumn("topMovies", task5.row_number().over(w)).filter(task5.col("topMovies") <= 5)\
    .orderBy("primaryName")\
    .drop("topMovies")

write_in_single_csv_file(directorsWithTop5movies, "..\\..\\Output\\directorsWithMovies")

task5.spark.stop()
