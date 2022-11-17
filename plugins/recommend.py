import os
import re
import pandas as pd
import urllib.request
import json
import Levenshtein as lev


def look_for_new_input():
    """looks for new input in the input file"""
    if os.path.exists("io/input.txt"):
        with open("io/input.txt", "r") as f:
            inp = f.read()
        return True, inp
    else:
        return False, None


def parse_input(ti):
    """Parses the input and returns the type and title"""
    _, inp = ti.xcom_pull(task_ids="look_for_new_input")
    inp = inp.lower()
    type_, title = inp.split(",")
    return type_, title


def extract():
    """Extracts the movies and series ids"""
    ids = pd.read_csv("data/movies_and_series_ids.csv")
    ids.dropna(inplace=True)
    return ids


def filter_by_type(ti):
    """Filters the dataset based on type USING REGEX."""

    def regex_is_type(x):
        return re.search(type_, x) is not None

    [type_, _], ids = ti.xcom_pull(task_ids=["parse_input", "extract"])

    return ids[ids["type"].apply(regex_is_type)]


def search_title_id(ti):
    """Finds the most similar titles using the levenshtein distance.
    Returns the ids dataframe sorted by similarity"""

    def dist(x):
        d1 = lev.distance(title.lower(), x.lower())
        d2 = lev.distance(title, x)
        return d1 + d2

    [_, title], ids = ti.xcom_pull(task_ids=["parse_input", "filter_by_type"])
    ids["dist"] = ids["original_title"].apply(dist)
    ids.sort_values(by="dist", inplace=True, ascending=True)
    ids.reset_index(inplace=True)
    return ids


def get_metadata(id_, type_):
    """
    Returns the data of the movie or series with the given index
    """
    api_key = "5165a765c111613fd9c8f5052a353fad"
    api_url = "https://api.themoviedb.org/3/"
    response = urllib.request.urlopen(
        api_url + f"{type_}/" + str(id_) + "?api_key=" + api_key
    )
    metadata = json.loads(response.read())
    response = urllib.request.urlopen(
        api_url + f"{type_}/" + str(id_) + "/credits?api_key=" + api_key
    )
    credits_dictionary = json.loads(response.read())
    metadata["credits"] = credits_dictionary
    title = (
        metadata["original_title"]
        if "original_title" in metadata
        else metadata["original_name"]
    )
    release_date = (
        metadata["release_date"]
        if "release_date" in metadata
        else metadata["first_air_date"]
    )
    genres = [genre["name"] for genre in metadata["genres"]]
    cast = [person["name"] for person in metadata["credits"]["cast"]][:1]
    director = [
        person["name"]
        for person in metadata["credits"]["crew"]
        if person["job"] == "Director"
    ]
    creators = (
        [person["name"] for person in metadata["created_by"]]
        if "created_by" in metadata
        else []
    )
    return {
        "title": title,
        "type": type_,
        "release_date": release_date,
        "genres": genres,
        "cast": cast,
        "director": director,
        "creators": creators,
        "id": id_,
    }


def get_recommendations(ti):
    """Iterates over most similar titles until it gets a recommendation"""
    ids = ti.xcom_pull(task_ids="search_title_id")
    type_, _ = ti.xcom_pull(task_ids="parse_input")
    i = 0
    recommendations = []
    original_title = ""
    while len(recommendations) == 0:
        id_ = ids.loc[i, "id"]
        original_title = ids.loc[i, "original_title"]
        api_key = "5165a765c111613fd9c8f5052a353fad"
        api_url = "https://api.themoviedb.org/3/"
        type_ = "movie" if type_ == "movie" else "tv"
        response = urllib.request.urlopen(
            api_url + f"{type_}/" + str(id_) + "/recommendations?api_key=" + api_key
        )
        recommendations = json.loads(response.read())
        recommendations = recommendations["results"]
        recommendations = [
            get_metadata(recommendation["id"], type_)
            for recommendation in recommendations
        ]
        i += 1
    metadata = get_metadata(id_, type_)
    return recommendations, metadata


def write_recs_to_file(ti):
    """Writes the recommendations to the output file"""
    recommendations, metadata = ti.xcom_pull(task_ids="get_recommendations")
    type_, title = ti.xcom_pull(task_ids="parse_input")
    with open("io/output.txt", "w") as f:
        f.write(f"Best match for {type_} '{title}':" + "\n")
        f.write(" --> " + summary(metadata) + " <-- " + "\n")
        f.write(f"\nRecommendations for {metadata['title']}:" + "\n")
        for recommendation in recommendations:
            f.write(" - " + summary(recommendation) + "\n")


def summary(film):
    """Returns a summary of the film"""
    dir_crea = "director" if film["type"] == "movie" else "creators"
    return f"{film['title']} ({film['release_date']}) by {film[dir_crea][0] if film[dir_crea] else 'Unknown'}"
