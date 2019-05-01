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
    """A joke recommendation engine
    """

    def __train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="jokeId", ratingCol="rating",
                  coldStartStrategy="drop")
        self.model = self.als.fit(self.ratingsdf)
        logger.info("ALS model built!")

    def get_top_ratings(self, user_id, jokes_count):
        """Recommends up to jokes_count top unrated jokes to user_id
        """
        users = self.ratingsdf.select(self.als.getUserCol())
        users = users.filter(users.userId == user_id)
        userSubsetRecs = self.model.recommendForUserSubset(users, jokes_count)
        userSubsetRecs = userSubsetRecs.withColumn("recommendations", explode("recommendations"))
        userSubsetRecs = userSubsetRecs.select(func.col('userId'),
                                               func.col('recommendations')['jokeId'].alias('jokeId'),
                                               func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                    drop('recommendations')
        userSubsetRecs = userSubsetRecs.drop('Rating')
        userSubsetRecs = userSubsetRecs.join(self.jokesdf, ("jokeId"), 'inner')
        # userSubsetRecs.show()
        # userSubsetRecs.printSchema()
        userSubsetRecs = userSubsetRecs.toPandas()
        userSubsetRecs = userSubsetRecs.to_json()
        return userSubsetRecs

    def get_top_joke_recommend(self, joke_id, user_count):
        """Recommends up to jokes_count top unrated jokes to user_id
        """
        jokes = self.ratingsdf.select(self.als.getItemCol())
        jokes = jokes.filter(jokes.jokeId == joke_id)
        jokeSubsetRecs = self.model.recommendForItemSubset(jokes, user_count)
        jokeSubsetRecs = jokeSubsetRecs.withColumn("recommendations", explode("recommendations"))
        jokeSubsetRecs = jokeSubsetRecs.select(func.col('jokeId'),
                                                 func.col('recommendations')['userId'].alias('userId'),
                                                 func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                        drop('recommendations')
        jokeSubsetRecs = jokeSubsetRecs.drop('Rating')
        jokeSubsetRecs = jokeSubsetRecs.join(self.jokesdf, ("jokeId"), 'inner')
        # userSubsetRecs.show()
        # userSubsetRecs.printSchema()
        jokeSubsetRecs = jokeSubsetRecs.toPandas()
        jokeSubsetRecs = jokeSubsetRecs.to_json()
        return jokeSubsetRecs

    def get_ratings_for_joke_ids(self, user_id, joke_id):
        """Given a user_id and a list of joke_ids, predict ratings for them
        """
        request = self.spark_session.createDataFrame([(user_id, joke_id)], ["userId", "jokeId"])
        ratings = self.model.transform(request).collect()
        return ratings

    def add_ratings(self, user_id, joke_id, ratings_given):
        """Add additional joke ratings in the format (user_id, joke_id, rating)
        """
        # Convert ratings to an RDD
        new_ratings = self.spark_session.createDataFrame([(user_id, joke_id, ratings_given)],
                                                         ["userId", "jokeId", "rating"])
        # Add new ratings to the existing ones
        self.ratingsdf = self.ratingsdf.union(new_ratings)
        # Re-train the ALS model with the new ratings
        self.__train_model()
        new_ratings = new_ratings.toPandas()
        new_ratings = new_ratings.to_json()
        return new_ratings

    def get_history(self, user_id):
        """Get rating history for a user
        """
        self.ratingsdf.createOrReplaceTempView("ratingsdata")
        user_history = self.spark_session.sql('SELECT userId, jokeId, rating from ratingsdata where userId = "%s"' %user_id)
        user_history = user_history.join(self.jokesdf, ("jokeId"), 'inner')
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
        ratings_file_path = os.path.join(dataset_path, 'jester_ratings.csv')
        self.ratingsdf = spark_session.read.csv(ratings_file_path, header=True, inferSchema=True).na.drop()
        # Load jokes data for later use
        logger.info("Loading Jokes data...")
        jokes_file_path = os.path.join(dataset_path, 'jester_items.csv')
        self.jokesdf = spark_session.read.csv(jokes_file_path, header=True, inferSchema=True).na.drop()
        # Train the model
        self.__train_model()
