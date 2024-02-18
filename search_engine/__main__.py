import sys
import os

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session

sys.path.extend([os.getcwd()])
from models import *
from search import *

import traceback
import argparse
from flask import Flask, jsonify, request
from flask_cors import CORS
import logging

logger: logging.Logger = logging.getLogger("SearchEngine")


def main(args) -> None:
    crawlopts: CrawlConfig = CrawlConfig.load_config(args.config)
    profiles: List[ProfileConfig] = ProfileConfig.load_profiles(args.config)
    sessionmakers: Dict[str, sessionmaker] = {}

    if crawlopts.debug:
        logging.basicConfig(
            level=logging.DEBUG, format=LOGGING_FORMAT, filename=crawlopts.log_file
        )
    else:
        logging.basicConfig(
            level=logging.INFO, format=LOGGING_FORMAT, filename=crawlopts.log_file
        )

    for profile in profiles:
        engine: Engine = create_engine(
            "sqlite:///"
            + os.path.join(crawlopts.database_dir, profile.profile_name)
            + ".db"
        )
        sessionmakers[profile.profile_name] = sessionmaker(bind=engine)

    searcher: Search = Search(crawlopts)
    app = Flask(__name__)
    CORS(app, resources={r"/search": {"origins": "*"}})

    def get_hash_info(hash: str):
        for _, sessionmaker in sessionmakers.items():
            session: Session = sessionmaker()

            result = session.query(URLData).filter(URLData.hash == hash).first()
            if result:
                return (
                    str(result.url),
                    int(result.time),
                    str(result.profile_name),
                    str(result.title),
                )
            
            session.close()

    @app.route("/search")
    def search():
        try:
            query = request.args.get("search").casefold()
            results = []

            for hash in searcher.search(query):
                url, timestamp, profile, title = get_hash_info(hash)
                results.append(
                    {
                        "url": url,
                        "timestamp": timestamp,
                        "profile": profile,
                        "title": title,
                    }
                )

            return jsonify(results)
        except Exception as err:
            logger.error("search failed with: %s", err)
            logger.error("%s", traceback.format_exc())
            return jsonify({})

    if crawlopts.debug:
        app.run(host="localhost", port=8000, debug=True)
    else:
        from waitress import serve
        serve(app=app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-config", help="Path to the config file", required=True)
    args = parser.parse_args()

    main(args)
