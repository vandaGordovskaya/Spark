"""
    Job finds top-10 movies in each genre and writes results in a single .csv file.
"""

from spark_tasks import task2_top_movies_by_genre

from spark_tasks.manageDFs import write_in_single_csv_file
from spark_tasks.task1_top_movies.top100movies import get_movies_with_rating

moviesWithRatingAndGenre = get_movies_with_rating()

highRatedMoviesWithGenre = moviesWithRatingAndGenre.filter(moviesWithRatingAndGenre.numVotes >= 100000)

all_sets_of_genres_in_map = highRatedMoviesWithGenre.select("genres").rdd.map(lambda r: r[0]).collect()
list_genres = ",".join(all_sets_of_genres_in_map)
list_genres = list(set(list_genres.split(',')))
genresDF = task2_top_movies_by_genre.sqlContext.createDataFrame([(l,) for l in list_genres], ['genre'])

topMoviesInGenre = highRatedMoviesWithGenre \
    .join(genresDF, highRatedMoviesWithGenre.genres.contains(genresDF.genre)) \
    .drop(highRatedMoviesWithGenre.genres)

w = task2_top_movies_by_genre.Window.partitionBy("genre") \
    .orderBy(task2_top_movies_by_genre.col("averageRating").desc())

top10moviesInGenre = topMoviesInGenre.withColumn("hot_top_in_genre", task2_top_movies_by_genre.row_number().over(w)) \
    .filter(task2_top_movies_by_genre.col("hot_top_in_genre") <= 10)

write_in_single_csv_file(top10moviesInGenre, "..\\..\\Output\\top10moviesGenres")
