#!/usr/bin/python3

import os
import logging
import psycopg2
import psycopg2.extras
import psycopg2.extensions

import joblib.joblib


RABBIT = {
    "host": os.environ.get("RABBITMQ_HOST"),
    "port": os.environ.get("RABBITMQ_PORT"),
    "credentials": os.environ.get("RABBITMQ_CREDS")
}

DB = {
    "host": os.environ.get("POSTGRESQL_HOST") or "vaindock_postgres_web",
    "port": os.environ.get("POSTGRESQL_PORT") or 5432,
    "user": os.environ.get("POSTGRESQL_USER") or "vainweb",
    "password": os.environ.get("POSTGRESQL_PASSWORD") or "vainweb",
    "dbname": os.environ.get("POSTGRESQL_DB") or "vainsocial-web"
}


class Processor(joblib.joblib.Worker):
    def __init__(self):
        super().__init__(jobtype="process")

    def connect(self, rabbit, db):
        """Connect to database."""
        logging.warning("connecting to database")
        super().connect(**rabbit)
        self._con = psycopg2.connect(**db)

    def commit(self, failed):
        self._con.commit()

    def work(self, payload):
        """Finish a job."""
        obj_id   = payload["id"]
        obj_type = payload["type"]
        o = payload["data"]["attributes"]
        r = payload["data"].get("relationships")

        c = self._con.cursor(
            cursor_factory=psycopg2.extras.DictCursor)

        d = {}
        if obj_type == "match":
            d["api_id"] = obj_id
            d["created_at"] = o["createdAt"]
            d["duration"] = o["duration"]
            d["game_mode"] = o["gameMode"]
            d["patch_version"] = o.get("patchVersion") or "2.2"
            d["shard_id"] = o["shardId"]
            d["end_game_reason"] = o["stats"]["endGameReason"]
            d["queue"] = o["stats"]["queue"]
            d["roster_1"] = r["rosters"]["data"][0]["id"]
            d["roster_2"] = r["rosters"]["data"][1]["id"]

        if d == {}:
            raise Exception
            # TODO

        # TODO player upsert
        # TODO request compile & preload
        l = [(c, v) for c, v in d.items()]
        columns = ",".join([t[0] for t in l])
        values = tuple([t[1] for t in l])
        c.execute("INSERT INTO " + obj_type + "(%s) VALUES %s",
                  ([psycopg2.extensions.AsIs(columns)] + [values]))
        c.close()


def startup():
    worker = Processor()
    worker.connect(RABBIT, DB)
    worker.setup()
    worker.run()

logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    startup()
