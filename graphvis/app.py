from flask import Flask, render_template, send_file
from flask_bootstrap import Bootstrap5
from waitress import serve
from flask_minify import Minify
import ujson as json
import tempfile
import os


def create_app(graph_path, debug=False):
    app = Flask(__name__)
    Bootstrap5(app)

    @app.route("/graph")
    def serve_graph():
        return send_file(graph_path, mimetype="application/json")

    @app.route("/")
    def main():
        return render_template(
            "graph.html",
            context={
                "profile": os.path.basename(graph_path).strip(".json"),
                "timestamp": os.path.basename(os.path.dirname(graph_path)),
            },
        )

    if not debug:
        Minify(app, html=True, js=True, cssless=True)
        serve(app, host="0.0.0.0", port=8000)
    else:
        app.run(host="0.0.0.0", port=8000, debug=True)
