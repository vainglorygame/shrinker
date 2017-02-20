#!/usr/bin/python

import os
import glob
import logging
import asyncio
import asyncpg

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


class Processor(object):
    def __init__(self):
        self._queries = {}
        self._srcpool = self._destpool = None

    async def setup(self):
        """Load .sql files from queries/ directory.
        File name is the table to insert into."""
        self._queries = {}
        scriptroot = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__)))
        # glob *.sql
        for fp in glob.glob(scriptroot + "/queries/*.sql"):
            # utf-8-sig is used by pgadmin, doesn't hurt to specify
            with open(fp, "r", encoding="utf-8-sig") as file:
                table = os.path.splitext(os.path.basename(fp))[0]
                logging.info("loaded query for '%s'", table)
                self._queries[table] = file.read()

        # prepare worker queue
        async with self._srcpool.acquire() as con:
            await con.execute("""
                CREATE TABLE IF NOT EXISTS processjobs
                    (id SERIAL, tablename TEXT, finished BOOL)
                """)
            # purge failed jobs from last time and retry
            await con.execute("DELETE FROM processjobs WHERE finished=false")

    async def connect(self, source, dest):
        """Connect to database by arguments."""
        self._srcpool = await asyncpg.create_pool(**source)
        self._destpool = await asyncpg.create_pool(**dest)

    async def acquire_job(self):
        """Reserve a job and return (id, table)."""
        async with self._srcpool.acquire() as con:
            while True:
                try:
                    async with con.transaction(isolation="serializable"):
                        # select us our job

                        table_name = await con.fetchval("""
                        SELECT table_name
                        FROM (
                          SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND
                          (table_name ~ '^(match|roster|participant)_\w{2,3}_\d{4}_\d{2}_\d{2}$'
                           OR table_name ~ '^(player|team)_\w{2,3}$')
                        ) AS table_names
                        WHERE table_name NOT IN
                        (SELECT tablename FROM processjobs)
                        """)
                        if table_name == None:
                            logging.warn("no jobs available")
                            return (None, None)

                        # store our job as pending
                        jobid = await con.fetchval("""
                            INSERT INTO processjobs(tablename, finished)
                            VALUES ($1, FALSE)
                            RETURNING id
                        """, table_name)
                        # exit loop
                        return (jobid, table_name)
                except asyncpg.exceptions.SerializationError:
                    # job is being picked up by another worker, try again
                    pass

    async def run_job(self):
        """Processes all data in table `table_name`."""
        # reserve one job
        jobid, table_name = await self.acquire_job()
        assert jobid is not None
        logging.info("%s: processing '%s'", jobid, table_name)

        # process.
        query_name = table_name.split("_")[0]  # `match`
        async with self._srcpool.acquire() as srccon:
            async with self._destpool.acquire() as destcon:
                # load SQL
                # replace generic `FROM` by `FROM partition`
                query = self._queries[query_name].replace(
                    "FROM \"" + query_name + "\"",
                    "FROM \"" + table_name + "\""
                )
                logging.debug("%s: using query '%s'", jobid, query_name)
                async with srccon.transaction():
                    async with destcon.transaction():
                        async for rec in srccon.cursor(query):
                            # loop over src, insert into dest
                            await self.into(destcon, rec, query_name)

    async def run(self):
        """Execute a function for each row that is fetched with query."""
        async def worker():
            """Spawn tasks forever."""
            try:
                await self.run_job()
            except AssertionError:
                logging.info("worker idling")
                await asyncio.sleep(30)
            asyncio.ensure_future(worker())

        for _ in range(5):
            asyncio.ensure_future(worker())

    async def into(self, conn, data, table):
        """Insert a named tuple into a table."""
        items = list(data.items())
        keys, values = [x[0] for x in items], [x[1] for x in items]
        placeholders = ["${}".format(i) for i, _ in enumerate(values, 1)]
        query = "INSERT INTO {} (\"{}\") VALUES ({})".format(
            table, "\", \"".join(keys), ", ".join(placeholders))
        await conn.execute(query, (*data))


async def main():
    pr = Processor()
    await pr.connect(source_db, dest_db)
    await pr.setup()
    await pr.run()

logging.basicConfig(level=logging.DEBUG)
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.run_forever()
