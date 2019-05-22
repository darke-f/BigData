from flask import Blueprint

main = Blueprint('main', __name__)

import json
from engine import RecommendationEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request


@main.route("/<int:model>/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(model, user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_rated = recommendation_engine.get_top_ratings(model, user_id, count)
    return json.dumps(top_rated)


@main.route("/<int:model>/movies/<int:movie_id>/recommend/<int:count>", methods=["GET"])
def movie_recommending(model, movie_id, count):
    logger.debug("MovieId %s TOP user recommending", movie_id)
    top_rated = recommendation_engine.get_top_movie_recommend(model, movie_id, count)
    return json.dumps(top_rated)


@main.route("/<int:model>/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
def movie_ratings(model, user_id, movie_id):
    logger.debug("User %s rating requested for movie %s", user_id, movie_id)
    ratings = recommendation_engine.get_ratings_for_movie_ids(model, user_id, movie_id)
    return json.dumps(ratings)

@main.route("/<int:model>/<int:user_id>/history", methods=["GET"])
def ratings_history(model, user_id):
    logger.debug("History for user %s is requested", user_id)
    user_history = recommendation_engine.get_history(model, user_id)
    return json.dumps(user_history)

@main.route("/<int:model>/<int:user_id>/giverating", methods=["POST"])
def add_ratings(model, user_id):
    # get the ratings from the Flask POST request object
    movieId_fetched = int(request.form.get('movieId'))
    ratings_fetched = float(request.form.get('ratingGiven'))
    # add them to the model using then engine API
    new_rating = recommendation_engine.add_ratings(model, user_id, movieId_fetched, ratings_fetched)

    return json.dumps(new_rating)


def create_app(spark_session, dataset_path):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_session, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app
