#!/usr/bin/python3

import asyncio
import os
import datetime
import glob
import logging
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


def date2iso(d):
    """Convert datetime to iso8601 zulu string."""
    date = d.replace(microsecond=0)
    date = date.isoformat()
    date += "Z"
    return date


class Processor(joblib.worker.Worker):
    def __init__(self, do_preload=False, do_analyze=False):
        self._queries = {}
        super().__init__(jobtype="process")
        self._do_preload = do_preload  # request preload jobs
        self._do_analyze = do_analyze  # request machine learning
        logging.debug("preload: %s, analyze: %s",
                      do_preload, do_analyze)

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
                self._queries[table] = await self._srccon.prepare(
                    file.read())
                logging.info("loaded query '%s'", table)
        self._deletematch = await self._srccon.prepare(
            "DELETE FROM match WHERE id=$1")

    async def _windup(self):
        self._srctr = self._srccon.transaction()
        self._desttr = self._destcon.transaction()
        await self._srctr.start()
        await self._desttr.start()
        self._priorities = []
        self._compilejobs = []
        self._analyzejobs = []
        self._preloads = []

    async def _teardown(self, failed):
        if failed:
            await self._srctr.rollback()
            await self._desttr.rollback()
        else:
            await self._srctr.commit()
            await self._desttr.commit()

            await self.request(
                jobtype="compile",
                payload=self._compilejobs,
                priority=self._priorities)
            if self._do_analyze:
                await self.request(
                    jobtype="analyze",
                    payload=self._analyzejobs)

            if self._do_preload:
                preloadjobs = [{
                    "region": s[0],
                    "params": {
                        "filter[playerNames]": s[1],
                        "filter[createdAt-start]": date2iso(s[2] + datetime.timedelta(seconds=1)),
                        "filter[gameMode]": "casual,ranked"
                    }
                } for s in self._preloads]
                await self.request(
                    jobtype="preload",
                    payload=preloadjobs,
                    priority=[2]*len(preloadjobs))

    async def _execute_job(self, jobid, payload, priority):
        """Finish a job."""
        object_id = payload["id"]
        explicit_player = payload.get("playername") or ""
        # 1 object in raw : n objects in web
        for table, query in self._queries.items():
            logging.debug("%s: running '%s' query",
                          jobid, table)
            # fetch from raw, converted to format for web table
            datas = await query.fetch(object_id)
            for data in datas:
                lmcd = None
                try:
                    if table == "player":
                        obj_id, lmcd = await self._playerinto(
                            self._destcon, data, table,
                            data["name"] == explicit_player)
                    else:
                        obj = await self._into(
                            self._destcon, data, table)
                        obj_id = None
                        if obj is not None:
                            obj_id = obj["api_id"]
                except asyncpg.exceptions.DeadlockDetectedError:
                    logging.error("%s: deadlocked!", jobid)
                    raise joblib.worker.JobFailed("deadlock",
                                                  True)  # critical
                except asyncpg.exceptions.IntegrityConstraintViolationError as err:
                    logging.error("%s: SQL error '%s'!", jobid, err)
                    raise joblib.worker.JobFailed({"id": object_id,
                                                   "error": str(err)}, True)

                logging.debug("record processed")
                if obj_id is not None:
                    # run web->web queries
                    payload = {
                        "type": table,
                        "id": obj_id
                    }
                    logging.debug("%s: requesting jobs for %s",
                                  jobid, obj_id)
                    self._priorities.append(priority)
                    if table == "participant":
                        self._compilejobs.append(payload)
                    if table == "player" and data["name"] == explicit_player:
                        self._compilejobs.append(payload)
                    if table == "participant":
                        self._analyzejobs.append(payload)

                    if lmcd is not None:
                        now = datetime.datetime.now()
                        interval_mins = (now - lmcd).total_seconds()/60
                        logging.debug("%s: minutes since last match: %s",
                                      jobid, interval_mins)
                        if priority == 1 and interval_mins > 30:
                            # TODO move to config var        ^
                            # TODO
                            # only allowed 1 level by ToS
                            if data["name"] not in [p[1] for p in self._preloads]:
                                # prevent duplicates
                                # TODO also prevent across batches!
                                self._preloads.append((data["shard_id"],
                                                       data["name"], lmcd))

        await self._deletematch.fetchrow(object_id)

    async def _playerinto(self, conn, data, table, update_date):
        """Upsert a player named tuple into a table.
        Return the object id."""
        # save lmcd to restore later
        lmcd = await conn.fetchval("""
            SELECT last_match_created_date FROM player
            WHERE api_id=$1
        """, data["api_id"])

        obj = await self._into(conn, data, table, conflict="""
            DO UPDATE SET ("{1}") = ({2})
            WHERE COALESCE(player.last_match_created_date,
                           'epoch'::TIMESTAMP)
            <= COALESCE(EXCLUDED.last_match_created_date,
                       'epoch'::TIMESTAMP)
            RETURNING api_id, last_match_created_date
        """)
        if obj is None:
            logging.debug("player was not updated")
            return None, None

        objid = obj["api_id"]
        objlmcd = obj["last_match_created_date"]

        # restore lmcd because
        # we want to request a preload job
        if not update_date:
            await conn.fetchval("""
                UPDATE player SET last_match_created_date=$2
                WHERE player.api_id=$1
            """, objid, lmcd)

        return objid, objlmcd

    async def _into(self, conn, data, table,
                    conflict="DO UPDATE SET (\"{1}\") = ({2}) " +
                             "RETURNING api_id"):
        """Insert a named tuple into a table.
        Return the object id."""
        items = list(data.items())
        keys, values = [x[0] for x in items], [x[1] for x in items]
        placeholders = ["${}".format(i) for i, _ in enumerate(values, 1)]
        query = ("INSERT INTO {0} (\"{1}\") VALUES ({2}) ON CONFLICT(api_id) " +
                 conflict).format(
                    table,
                    "\", \"".join(keys),
                    ", ".join(placeholders))
        logging.debug("query: %s", query)
        logging.debug("data: %s", data)
        return await conn.fetchrow(query, (*data))


async def startup():
    worker = Processor(
        do_preload=os.environ.get("VAINSOCIAL_SPIDER")=="true",
        do_analyze=os.environ.get("VAINSOCIAL_ANALYZE")=="true"
    )
    await worker.connect(
        source_db, dest_db
    )
    await worker.setup()
    await worker.run(batchlimit=1000)

logging.basicConfig(level=logging.DEBUG)

loop = asyncio.get_event_loop()
loop.run_until_complete(startup())
