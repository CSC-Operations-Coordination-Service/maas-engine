"""Health check of the replay worker: 200 while every lane thread runs"""

from flask import Flask


def build_app(worker) -> Flask:
    """A Flask app answering `/` with the worker's state

    The worker has no AMQP connection to probe between requests (it holds none while
    a replay runs): its liveness is its lane threads.
    """
    app = Flask("maas_engine_replay_health")

    @app.route("/")
    def status():
        if worker.is_alive():
            return "ok", 200
        return "a lane thread stopped", 500

    return app
