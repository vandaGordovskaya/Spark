"""
    Job finds top-100 movies with at least 100 000 votes:
       - top100movies - results for all period;
       - top100movies_last10years - for the last 10 years;
       - top100movies_60s - within popular films in 60s.
"""

from spark_tasks import task1_top_movies
from spark_tasks.manageDFs import create_df, write_in_csv_file

title_basicsDF = create_df("..\\..\\Datasets\\title.basics.tsv.gz")

ratingsDF = create_df("..\\..\\Datasets\\title.ratings.tsv.gz")


def get_movies_with_rating():
    ratedMovies = title_basicsDF.filter(title_basicsDF.titleType == "movie") \
        .join(ratingsDF, title_basicsDF.tconst == ratingsDF.tconst, 'inner') \
        .select(title_basicsDF.tconst,
                title_basicsDF.primaryTitle,
                ratingsDF.numVotes,
                ratingsDF.averageRating,
                title_basicsDF.startYear,
                title_basicsDF.genres)
    return ratedMovies


moviesWithRating = get_movies_with_rating().drop("genres")

top100movies = moviesWithRating.filter(moviesWithRating.numVotes >= 100000) \
    .orderBy(task1_top_movies.col('averageRating').desc()).limit(100)

write_in_csv_file(top100movies, "..\\..\\Output\\top100movies")

top100movies_last10years = moviesWithRating.filter((top100movies.startYear <= "2021")
                                                   & (top100movies.startYear >= "2011")
                                                   & (moviesWithRating.numVotes >= 100000)) \
    .orderBy(task1_top_movies.col('averageRating').desc()).limit(100)

write_in_csv_file(top100movies_last10years, "..\\..\\Output\\top100moviesLast10Years")

top100movies_60s = moviesWithRating.filter((top100movies.startYear >= "1960")
                                           & (top100movies.startYear < "1970")
                                           & (moviesWithRating.numVotes >= 100000)) \
    .orderBy(task1_top_movies.col('averageRating').desc()).limit(100)

write_in_csv_file(top100movies_60s, "..\\..\\Output\\top100movies60s")
