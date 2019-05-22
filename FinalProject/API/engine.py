import os
import logging
import pandas as pd

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import types
from pyspark.sql.functions import explode
import pyspark.sql.functions as func

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationEngine:
    """A movie recommendation engine
    """

    def __train_all_model(self):
        """Train the ALS model with the current dataset
        """
        
        logger.info("Splitting dataset")
        
        self.df0 = self.df.limit(int(self.dataset_count / 3))
        self.df1 = self.df.limit(int(self.dataset_count * 2 / 3))
        self.df2 = self.df
        
        print('df 0 count = ' + str(self.df0.count()))
        print('df 1 count = ' + str(self.df1.count()))
        print('df 2 count = ' + str(self.df2.count()))
        logger.info("Dataset Splitted !")
        
        #Model 1
        logger.info("Training the ALS model 1")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
                  coldStartStrategy="drop")
        self.model1 = self.als.fit(self.df0)
        logger.info("ALS model 1 built!")
        
        #Model 2
        logger.info("Training the ALS model 2")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
                  coldStartStrategy="drop")
        self.model2 = self.als.fit(self.df1)
        logger.info("ALS model 2 built!")
        
        #Model 3
        logger.info("Training the ALS model 3")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
                  coldStartStrategy="drop")
        self.model3 = self.als.fit(self.df2)
        logger.info("ALS model 3 built!")
        
    def __train_model(self, model):
        """Train the ALS model with the current dataset
        """
        
        logger.info("Training the ALS model...")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
                  coldStartStrategy="drop")
        if model == 0:
            self.model1 = self.als.fit(self.df0)
        elif model == 1:
            self.model2 = self.als.fit(self.df1)
        elif model == 2:
            self.model3 = self.als.fit(self.df2)
        logger.info("ALS model built!")

    def get_top_ratings(self, model, user_id, movies_count):
        """Recommends up to movies_count top unrated movies to user_id
        """
        
        if model == 0:
            users = self.df0.select(self.als.getUserCol())
            users = users.filter(users.userId == user_id)
            userSubsetRecs = self.model1.recommendForUserSubset(users, movies_count)
            userSubsetRecs = userSubsetRecs.withColumn("recommendations", explode("recommendations"))
            userSubsetRecs = userSubsetRecs.select(func.col('userId'),
                                                   func.col('recommendations')['movieId'].alias('movieId'),
                                                   func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                        drop('recommendations')
            userSubsetRecs = userSubsetRecs.drop('Rating')
            userSubsetRecs = userSubsetRecs.join(self.moviesdf, ("movieId"), 'inner')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            userSubsetRecs = userSubsetRecs.toPandas()
            userSubsetRecs = userSubsetRecs.to_json()
            return userSubsetRecs
        elif model == 1:
            users = self.df1.select(self.als.getUserCol())
            users = users.filter(users.userId == user_id)
            userSubsetRecs = self.model2.recommendForUserSubset(users, movies_count)
            userSubsetRecs = userSubsetRecs.withColumn("recommendations", explode("recommendations"))
            userSubsetRecs = userSubsetRecs.select(func.col('userId'),
                                                   func.col('recommendations')['movieId'].alias('movieId'),
                                                   func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                        drop('recommendations')
            userSubsetRecs = userSubsetRecs.drop('Rating')
            userSubsetRecs = userSubsetRecs.join(self.moviesdf, ("movieId"), 'inner')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            userSubsetRecs = userSubsetRecs.toPandas()
            userSubsetRecs = userSubsetRecs.to_json()
            return userSubsetRecs
        elif model == 2:
            users = self.df2.select(self.als.getUserCol())
            users = users.filter(users.userId == user_id)
            userSubsetRecs = self.model3.recommendForUserSubset(users, movies_count)
            userSubsetRecs = userSubsetRecs.withColumn("recommendations", explode("recommendations"))
            userSubsetRecs = userSubsetRecs.select(func.col('userId'),
                                                   func.col('recommendations')['movieId'].alias('movieId'),
                                                   func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                        drop('recommendations')
            userSubsetRecs = userSubsetRecs.drop('Rating')
            userSubsetRecs = userSubsetRecs.join(self.moviesdf, ("movieId"), 'inner')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            userSubsetRecs = userSubsetRecs.toPandas()
            userSubsetRecs = userSubsetRecs.to_json()
            return userSubsetRecs

    def get_top_movie_recommend(self, model, movie_id, user_count):
        """Recommends up to movies_count top unrated movies to user_id
        """
        
        if model == 0:
            movies = self.df0.select(self.als.getItemCol())
            movies = movies.filter(movies.movieId == movie_id)
            movieSubsetRecs = self.model1.recommendForItemSubset(movies, user_count)
            movieSubsetRecs = movieSubsetRecs.withColumn("recommendations", explode("recommendations"))
            movieSubsetRecs = movieSubsetRecs.select(func.col('movieId'),
                                                     func.col('recommendations')['userId'].alias('userId'),
                                                     func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                            drop('recommendations')
            movieSubsetRecs = movieSubsetRecs.drop('Rating')
            movieSubsetRecs = movieSubsetRecs.join(self.moviesdf, ("movieId"), 'inner')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            movieSubsetRecs = movieSubsetRecs.toPandas()
            movieSubsetRecs = movieSubsetRecs.to_json()
            return movieSubsetRecs
        elif model == 1:
            movies = self.df1.select(self.als.getItemCol())
            movies = movies.filter(movies.movieId == movie_id)
            movieSubsetRecs = self.model2.recommendForItemSubset(movies, user_count)
            movieSubsetRecs = movieSubsetRecs.withColumn("recommendations", explode("recommendations"))
            movieSubsetRecs = movieSubsetRecs.select(func.col('movieId'),
                                                     func.col('recommendations')['userId'].alias('userId'),
                                                     func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                            drop('recommendations')
            movieSubsetRecs = movieSubsetRecs.drop('Rating')
            movieSubsetRecs = movieSubsetRecs.join(self.moviesdf, ("movieId"), 'inner')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            movieSubsetRecs = movieSubsetRecs.toPandas()
            movieSubsetRecs = movieSubsetRecs.to_json()
            return movieSubsetRecs
        elif model == 2:
            movies = self.df2.select(self.als.getItemCol())
            movies = movies.filter(movies.movieId == movie_id)
            movieSubsetRecs = self.model3.recommendForItemSubset(movies, user_count)
            movieSubsetRecs = movieSubsetRecs.withColumn("recommendations", explode("recommendations"))
            movieSubsetRecs = movieSubsetRecs.select(func.col('movieId'),
                                                     func.col('recommendations')['userId'].alias('userId'),
                                                     func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                            drop('recommendations')
            movieSubsetRecs = movieSubsetRecs.drop('Rating')
            movieSubsetRecs = movieSubsetRecs.join(self.moviesdf, ("movieId"), 'inner')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            movieSubsetRecs = movieSubsetRecs.toPandas()
            movieSubsetRecs = movieSubsetRecs.to_json()
            return movieSubsetRecs

    def get_ratings_for_movie_ids(self, model, user_id, movie_id):
        """Given a user_id and a list of movie_ids, predict ratings for them
        """
        
        if model == 0:
            request = self.spark_session.createDataFrame([(user_id, movie_id)], ["userId", "movieId"])
            ratings = self.model1.transform(request).collect()
            return ratings
        elif model == 1:
            request = self.spark_session.createDataFrame([(user_id, movie_id)], ["userId", "movieId"])
            ratings = self.model2.transform(request).collect()
            return ratings
        elif model == 2:
            request = self.spark_session.createDataFrame([(user_id, movie_id)], ["userId", "movieId"])
            ratings = self.model3.transform(request).collect()
            return ratings

    def add_ratings(self, model, user_id, movie_id, ratings_given):
        """Add additional movie ratings in the format (user_id, rating, movie_id)
        """
        
        if model == 0:
            # Convert ratings to an RDD
            new_ratings = self.spark_session.createDataFrame([(user_id, ratings_given, movie_id)],
                                                             ["userId", "rating", "movieId"])
            # Add new ratings to the existing ones
            self.df0 = self.df0.union(new_ratings)
            # Re-train the ALS model with the new ratings
            self.__train_model(model)
            new_ratings = new_ratings.toPandas()
            new_ratings = new_ratings.to_json()
            return new_ratings
        elif model == 1:
            # Convert ratings to an RDD
            new_ratings = self.spark_session.createDataFrame([(user_id, ratings_given, movie_id)],
                                                             ["userId", "rating", "movieId"])
            # Add new ratings to the existing ones
            self.df1 = self.df1.union(new_ratings)
            # Re-train the ALS model with the new ratings
            self.__train_model(model)
            new_ratings = new_ratings.toPandas()
            new_ratings = new_ratings.to_json()
            return new_ratings
        elif model == 2:
            # Convert ratings to an RDD
            new_ratings = self.spark_session.createDataFrame([(user_id, ratings_given, movie_id)],
                                                             ["userId", "rating", "movieId"])
            # Add new ratings to the existing ones
            self.df2 = self.df2.union(new_ratings)
            # Re-train the ALS model with the new ratings
            self.__train_model(model)
            new_ratings = new_ratings.toPandas()
            new_ratings = new_ratings.to_json()
            return new_ratings

    def get_history(self, model, user_id):
        """Get rating history for a user
        """
        
        if model == 0:
            self.df0.createOrReplaceTempView("ratingsdata")
            user_history = self.spark_session.sql('SELECT userId, movieId, rating from ratingsdata where userId = "%s"' %user_id)
            user_history = user_history.join(self.moviesdf, ("movieId"), 'inner')
            user_history = user_history.toPandas()
            user_history = user_history.to_json()
            return user_history
        elif model == 1:
            self.df1.createOrReplaceTempView("ratingsdata")
            user_history = self.spark_session.sql('SELECT userId, movieId, rating from ratingsdata where userId = "%s"' %user_id)
            user_history = user_history.join(self.moviesdf, ("movieId"), 'inner')
            user_history = user_history.toPandas()
            user_history = user_history.to_json()
            return user_history
        elif model == 2:
            self.df2.createOrReplaceTempView("ratingsdata")
            user_history = self.spark_session.sql('SELECT userId, movieId, rating from ratingsdata where userId = "%s"' %user_id)
            user_history = user_history.join(self.moviesdf, ("movieId"), 'inner')
            user_history = user_history.toPandas()
            user_history = user_history.to_json()
            return user_history

    def __init__(self, spark_session, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Recommendation Engine: ")
        self.spark_session = spark_session
        # Load ratings data for later use
        logger.info("Loading Ratings data...")
        
        file_counter = 0
        while True:
            file_name = 'data_part_' + str(file_counter) + '.txt'
            dataset_file_path = os.path.join(dataset_path, file_name)
            exist = os.path.isfile(dataset_file_path)
            if exist:
                if file_counter == 0:
                    self.df = spark_session.read.csv(dataset_file_path, header=None, inferSchema=True)
                else:
                    df_new = spark_session.read.csv(dataset_file_path, header=None, inferSchema=True)
                    self.df = self.df.union(df_new)
                self.dataset_count = self.df.count()
                print('Data loaded = ' + str(self.dataset_count))
                print(file_name + 'Loaded !')
                file_counter += 1
            else:
                break
        self.df = self.df.selectExpr("_c0 as userId", "_c1 as rating", "_c2 as movieId")
        self.df.show()
        # print(self.df.count())
        
        # Load movie data for later use
        logger.info("Loading Movie data...")
        movies_file_path = os.path.join(dataset_path, 'movie_titles.csv')
        self.moviesdf = spark_session.read.csv(movies_file_path, header=None, inferSchema=True)
        self.moviesdf = self.moviesdf.selectExpr("_c0 as movieId", "_c1 as Year", "_c2 as movie_title")
        # Train the model
        self.__train_all_model()
