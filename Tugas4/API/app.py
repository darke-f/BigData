from flask import Blueprint

main = Blueprint('main', __name__)

import json
from engine import RecommendationEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request


@main.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_rated = recommendation_engine.get_top_ratings(user_id, count)
    return json.dumps(top_rated)


@main.route("/jokes/<int:joke_id>/recommend/<int:count>", methods=["GET"])
def joke_recommending(joke_id, count):
    logger.debug("JokeId %s TOP user recommending", joke_id)
    top_rated = recommendation_engine.get_top_joke_recommend(joke_id, count)
    return json.dumps(top_rated)


@main.route("/<int:user_id>/ratings/<int:joke_id>", methods=["GET"])
def joke_ratings(user_id, joke_id):
    logger.debug("User %s rating requested for joke %s", user_id, joke_id)
    ratings = recommendation_engine.get_ratings_for_joke_ids(user_id, joke_id)
    return json.dumps(ratings)

@main.route("/<int:user_id>/history", methods=["GET"])
def ratings_history(user_id):
    logger.debug("History for user %s is requested", user_id)
    user_history = recommendation_engine.get_history(user_id)
    return json.dumps(user_history)

@main.route("/<int:user_id>/giverating", methods=["POST"])
def add_ratings(user_id):
    # get the ratings from the Flask POST request object
    jokeId_fetched = int(request.form.get('jokeId'))
    ratings_fetched = float(request.form.get('ratingGiven'))
    # add them to the model using then engine API
    new_rating = recommendation_engine.add_ratings(user_id, jokeId_fetched, ratings_fetched)

    return json.dumps(new_rating)


def create_app(spark_session, dataset_path):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_session, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app
