#!/usr/bin/python3

import asyncio
import os
import glob
import logging
import json
import asyncpg

import joblib.worker


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


class Processor(joblib.worker.Worker):
    def __init__(self, do_spider=False, do_analyze=False):
        self._srcpool = None
        self._destpool = None
        self._queries = {}
        super().__init__(jobtype="process")
        self._do_spider = do_spider
        self._do_analyze = do_analyze

    async def connect(self, sourcea, desta):
        """Connect to database."""
        logging.warning("connecting to database")
        await super().connect(**sourcea)
        self._srccon = await asyncpg.connect(**sourcea)
        self._destcon = await asyncpg.connect(**desta)

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

    async def _windup(self):
        self._srctr = self._srccon.transaction()
        self._desttr = self._destcon.transaction()
        await self._srctr.start()
        await self._desttr.start()
        self._compilejobs = []
        self._analyzejobs = []
        self._spiders = []

    async def _teardown(self, failed):
        if failed:
            await self._srctr.rollback()
            await self._desttr.rollback()
        else:
            await self._srctr.commit()
            await self._desttr.commit()
            self._compilejobs = list({
                # uniquify
                j["id"]: j for j in self._compilejobs
            }.values())

            await self._queue.request(
                jobtype="compile",
                payload=self._compilejobs)#,
#                priority=priority) TODO
            self._analyzejobs = list({
                # uniquify
                j["id"]: j for j in self._analyzejobs
            }.values())
            if self._do_analyze:
                await self._queue.request(
                    jobtype="analyze",
                    payload=self._analyzejobs)

            spiderjobs = [{
                "region": s[0],
                "params": {
                    "filter[playerNames]": s[1],
                    "filter[createdAt-start]": "2017-03-01T00:00:00Z"
                }
            } for s in self._spiders]
            if self._do_spider:
                await self._queue.request(
                    jobtype="grab",
                    payload=spiderjobs,
                    priority=1001)

    async def _execute_job(self, jobid, payload, priority):
        """Finish a job."""
        object_id = payload["id"]
        explicit_player = payload["playername"]
        # 1 object in raw : n objects in web
        for table, query in self._queries.items():
            logging.debug("%s: running '%s' query",
                          jobid, table)
            # fetch from raw, converted to format for web table
            # TODO refactor - duplicated messy code
            if table == "player":
                # upsert under special conditions
                datas = await self._srccon.fetch(
                    query, object_id, explicit_player)
                for data in datas:
                    try:
                        obj_id = await self._playerinto(
                            self._destcon, data, table,
                            data["name"] == explicit_player)
                    except asyncpg.exceptions.DeadlockDetectedError:
                        raise joblib.worker.JobFailed("deadlock")

                    logging.debug("record processed")
                    if obj_id:
                        # run web->web queries
                        payload = {
                            "type": table,
                            "id": obj_id
                        }
                        self._compilejobs.append(payload)
                        self._analyzejobs.append(payload)
                        if explicit_player != data["name"]:
                            self._spiders.append((data["shard_id"],
                                                 data["name"]))
            else:
                datas = await self._srccon.fetch(
                    query, object_id)
                for data in datas:
                    # insert processed result into web table
                    try:
                        obj_id = await self._into(
                            self._destcon, data, table)
                    except asyncpg.exceptions.DeadlockDetectedError:
                        raise joblib.worker.JobFailed("deadlock")
                    logging.debug("record processed")

                    if obj_id:
                        # run web->web queries
                        payload = {
                            "type": table,
                            "id": obj_id
                        }
                        self._compilejobs.append(payload)
                        self._analyzejobs.append(payload)

        await self._srccon.execute(
            "DELETE FROM match WHERE id=$1", object_id)

    async def _playerinto(self, conn, data, table, do_upsert_date):
        """Upsert a player named tuple into a table.
        Return the object id."""
        if do_upsert_date:
            # explicit update for this player -> store
            lmcd = data["last_match_created_date"]
        data = dict(data)
        del data["last_match_created_date"]

        items = list(data.items())
        keys, values = [x[0] for x in items], [x[1] for x in items]
        placeholders = ["${}".format(i) for i, _ in enumerate(values, 1)]
        # upsert all values except lmcd if they are more recent
        query = """
            INSERT INTO player ("{0}", "last_match_created_date")
            VALUES ({1}, 'epoch'::timestamp)
            ON CONFLICT("api_id") DO UPDATE SET ("{0}") = ({1})
            WHERE player.played < EXCLUDED.played
            RETURNING "api_id"
        """.format(
            "\", \"".join(keys), ", ".join(placeholders))
        objid = await conn.fetchval(query, *data.values())

        if do_upsert_date:
            # upsert lmcd because it was an explicit request
            await conn.execute("""
                UPDATE player SET "last_match_created_date"=$2
                WHERE player."api_id"=$1 AND
                player."last_match_created_date" < $2
            """, objid, lmcd)

        return objid

    async def _into(self, conn, data, table):
        """Insert a named tuple into a table.
        Return the object id."""
        items = list(data.items())
        keys, values = [x[0] for x in items], [x[1] for x in items]
        placeholders = ["${}".format(i) for i, _ in enumerate(values, 1)]
        query = "INSERT INTO {} (\"{}\") VALUES ({}) ON CONFLICT DO NOTHING RETURNING api_id".format(
            table, "\", \"".join(keys), ", ".join(placeholders))
        logging.debug(query)
        return await conn.fetchval(query, (*data))


async def startup():
    for _ in range(2):
        worker = Processor(
            do_spider=os.environ.get("VAINSOCIAL_SPIDER")=="true",
            do_analyze=os.environ.get("VAINSOCIAL_ANALYZE")=="true"
        )
        await worker.connect(
            source_db, dest_db
        )
        await worker.setup()
        await worker.start(batchlimit=50)

logging.basicConfig(
    filename=os.path.realpath(
        os.path.join(os.getcwd(),
                     os.path.dirname(__file__))) +
        "/logs/processor.log",
    filemode="a",
    level=logging.DEBUG
)
console = logging.StreamHandler()
console.setLevel(logging.WARNING)
logging.getLogger("").addHandler(console)

loop = asyncio.get_event_loop()
loop.run_until_complete(startup())
loop.run_forever()
