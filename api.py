#!/usr/bin/python3

import asyncio
import os
import glob
import logging
import json
import asyncpg

import joblib.worker

queue_db = {
    "host": os.environ.get("POSTGRESQL_SOURCE_HOST") or "vaindock_postgres_raw",
    "port": os.environ.get("POSTGRESQL_SOURCE_PORT") or 5532,
    "user": os.environ.get("POSTGRESQL_SOURCE_USER") or "vainraw",
    "password": os.environ.get("POSTGRESQL_SOURCE_PASSWORD") or "vainraw",
    "database": os.environ.get("POSTGRESQL_SOURCE_DB") or "vainsocial-raw"
}

db_config = {
    "host": os.environ.get("POSTGRESQL_DEST_HOST") or "vaindock_postgres_web",
    "port": os.environ.get("POSTGRESQL_DEST_PORT") or 5432,
    "user": os.environ.get("POSTGRESQL_DEST_USER") or "vainweb",
    "password": os.environ.get("POSTGRESQL_DEST_PASSWORD") or "vainweb",
    "database": os.environ.get("POSTGRESQL_DEST_DB") or "vainsocial-web"
}


class Compiler(joblib.worker.Worker):
    def __init__(self):
        self._pool = None
        self._queries = {}
        super().__init__(jobtype="compile")

    async def connect(self, dbconf, queuedb):
        """Connect to database."""
        logging.warning("connecting to database")
        await super().connect(**queuedb)
        self._pool = await asyncpg.create_pool(**dbconf)

    async def setup(self):
        """Initialize the database."""
        scriptroot = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__)))
        for path in glob.glob(scriptroot + "/queries/*/*.sql"):
            # utf-8-sig is used by pgadmin, doesn't hurt to specify
            # directory names: web target table
            table = os.path.basename(os.path.dirname(path))
            with open(path, "r", encoding="utf-8-sig") as file:
                try:
                    self._queries[table].append(file.read())
                except KeyError:
                    self._queries[table] = [file.read()]
                logging.info("loaded query '%s'", table)

    async def _execute_job(self, jobid, payload, priority):
        """Finish a job."""
        object_id = payload["id"]
        table = payload["type"]
        if table not in self._queries:
            return
        async with self._pool.acquire() as con:
            logging.debug("%s: compiling '%s' from '%s'",
                          jobid, object_id, table)
            for query in self._queries[table]:
                async with con.transaction():
                    await con.execute(query, object_id)


async def startup():
    worker = Compiler()
    await worker.connect(db_config, queue_db)
    await worker.setup()
    await worker.start(1)


logging.basicConfig(
    filename=os.path.realpath(
        os.path.join(os.getcwd(),
                 os.path.dirname(__file__))) +
        "/logs/compiler.log",
    filemode="a",
    level=logging.DEBUG
)
console = logging.StreamHandler()
console.setLevel(logging.WARNING)
logging.getLogger("").addHandler(console)

loop = asyncio.get_event_loop()
loop.run_until_complete(startup())
loop.run_forever()
