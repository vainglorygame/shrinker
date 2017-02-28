#!/usr/bin/python3

import asyncio
import os
import glob
import logging
import json
import asyncpg

import joblib.joblib


source_db = {
    "host": os.environ.get("POSTGRESQL_SOURCE_HOST") or "vaindock_postgres_raw",
    "port": os.environ.get("POSTGRESQL_SOURCE_PORT") or 5532,
    "user": os.environ.get("POSTGRESQL_SOURCE_USER") or "vainraw",
    "password": os.environ.get("POSTGRESQL_SOURCE_PASSWORD") or "vainraw",
    "database": os.environ.get("POSTGRESQL_SOURCE_DB") or "vainsocial-raw"
}

dest_db = {
    "host": os.environ.get("POSTGRESQL_DEST_HOST") or "vaindock_postgres_web",
    "port": os.environ.get("POSTGRESQL_DEST_PORT") or 5432,
    "user": os.environ.get("POSTGRESQL_DEST_USER") or "vainweb",
    "password": os.environ.get("POSTGRESQL_DEST_PASSWORD") or "vainweb",
    "database": os.environ.get("POSTGRESQL_DEST_DB") or "vainsocial-web"
}


class Worker(object):
    def __init__(self):
        self._queue = None
        self._srcpool = None
        self._destpool = None
        self._queries = {}

    async def connect(self, sourcea, desta):
        """Connect to database."""
        logging.info("connecting to database")
        self._queue = joblib.joblib.JobQueue()
        await self._queue.connect(**sourcea)
        await self._queue.setup()
        self._srcpool = await asyncpg.create_pool(**sourcea)
        self._destpool = await asyncpg.create_pool(**desta)

    async def setup(self):
        """Initialize the database."""
        scriptroot = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__)))
        for path in glob.glob(scriptroot + "/queries/*.sql"):
            # utf-8-sig is used by pgadmin, doesn't hurt to specify
            # file names: raw target table
            table = os.path.splitext(os.path.basename(path))[0]
            with open(path, "r", encoding="utf-8-sig") as file:
                self._queries[table] = file.read()
                logging.info("loaded query '%s'", table)

    async def _execute_job(self, jobid, payload, priority):
        """Finish a job."""
        object_id = payload["id"]
        explicit_player = payload["playername"]
        async with self._srcpool.acquire() as srccon:
            async with self._destpool.acquire() as destcon:
                async with srccon.transaction():
                    async with destcon.transaction():
                        # 1 object in raw : n objects in web
                        for table, query in self._queries.items():
                            logging.debug("%s: running '%s' query",
                                          jobid, table)
                            # fetch from raw, converted to format for web table
                            # TODO refactor - duplicated messy code
                            if table == "player":
                                # upsert under special conditions
                                datas = await srccon.fetch(
                                    query, object_id, explicit_player)
                                for data in datas:
                                    obj_id = await self._playerinto(
                                        destcon, data, table,
                                        data["name"] == explicit_player)

                                    if obj_id:
                                        # run web->web queries
                                        payload = {
                                            "type": table,
                                            "id": obj_id
                                        }
                                        await self._queue.request(
                                            jobtype="compile",
                                            payload=payload,
                                            priority=priority)
                            else:
                                datas = await srccon.fetch(
                                    query, object_id)
                                for data in datas:
                                    # insert processed result into web table
                                    obj_id = await self._into(
                                        destcon, data, table)

                                    if obj_id:
                                        # run web->web queries
                                        payload = {
                                            "type": table,
                                            "id": obj_id
                                        }
                                        await self._queue.request(
                                            jobtype="compile",
                                            payload=payload,
                                            priority=priority)

                    data = await srccon.fetchrow(
                        "DELETE FROM match WHERE id=$1", object_id)

    async def _playerinto(self, conn, data, table, do_upsert_date):
        """Upsert a player named tuple into a table.
        Return the object id."""
        if do_upsert_date:
            # explicit update for this player -> store
            lmcd = data["lastMatchCreatedDate"]
        data = dict(data)
        del data["lastMatchCreatedDate"]

        items = list(data.items())
        keys, values = [x[0] for x in items], [x[1] for x in items]
        placeholders = ["${}".format(i) for i, _ in enumerate(values, 1)]
        # upsert all values except lmcd if they are more recent
        query = """
            INSERT INTO player ("{0}", "lastMatchCreatedDate")
            VALUES ({1}, 'epoch'::timestamp)
            ON CONFLICT("apiId") DO UPDATE SET ("{0}") = ({1})
            WHERE player.played < EXCLUDED.played
            RETURNING id
        """.format(
            "\", \"".join(keys), ", ".join(placeholders))
        objid = await conn.fetchval(query, *data.values())

        if do_upsert_date:
            # upsert lmcd because it was an explicit request
            await conn.execute("""
                UPDATE player SET "lastMatchCreatedDate"=$1
                WHERE player."lastMatchCreatedDate" < $1
            """, lmcd)

        return objid

    async def _into(self, conn, data, table):
        """Insert a named tuple into a table.
        Return the object id."""
        items = list(data.items())
        keys, values = [x[0] for x in items], [x[1] for x in items]
        placeholders = ["${}".format(i) for i, _ in enumerate(values, 1)]
        query = "INSERT INTO {} (\"{}\") VALUES ({}) ON CONFLICT DO NOTHING RETURNING id".format(
            table, "\", \"".join(keys), ", ".join(placeholders))
        return await conn.fetchval(query, (*data))

    async def _work(self):
        """Fetch a job and run it."""
        jobid, payload, priority = await self._queue.acquire(
            jobtype="process")
        if jobid is None:
            raise LookupError("no jobs available")
        await self._execute_job(jobid, payload, priority)
        await self._queue.finish(jobid)

    async def run(self):
        """Start jobs forever."""
        while True:
            try:
                await self._work()
            except LookupError:
                await asyncio.sleep(1)

    async def start(self, number=1):
        """Start jobs in background."""
        for _ in range(number):
            asyncio.ensure_future(self.run())

async def startup():
    worker = Worker()
    await worker.connect(
        source_db, dest_db
    )
    await worker.setup()
    await worker.start(4)

logging.basicConfig(level=logging.DEBUG)
loop = asyncio.get_event_loop()
loop.run_until_complete(startup())
loop.run_forever()
