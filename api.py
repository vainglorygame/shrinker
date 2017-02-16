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

    def setup(self):
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

    async def connect(self, source, dest):
        """Connect to database by arguments."""
        self._srcpool = await asyncpg.create_pool(**source)
        self._destpool = await asyncpg.create_pool(**dest)

    async def run(self, stop_after=-1):
        """Execute a function for each row that is fetched with query."""
        # TODO unnest
        async with self._srcpool.acquire() as srccon:
            async with self._destpool.acquire() as destcon:
                for table, query in self._queries.items():
                    logging.info("running query for '%s'", table)
                    stop_after -= 1  # TODO for debugging, don't process whole table
                    async with srccon.transaction():
                        async with destcon.transaction():
                            rec_o = ""
                            try:
                                async for rec in srccon.cursor(query):
                                    rec_o = rec
                                    await self.into(destcon, rec, table)
                            except:
                                logging.error("rec: %s", rec_o)

    async def into(self, conn, data, table):
        """Insert a named tuple into a table."""
        items = list(data.items())
        keys, values = [x[0] for x in items], [x[1] for x in items]
        placeholders = ["${}".format(i) for i, _ in enumerate(values, 1)]
        query = "INSERT INTO {} (\"{}\") VALUES ({})".format(
            table, "\", \"".join(keys), ", ".join(placeholders))
        try:
            await conn.execute(query, (*data))
        except:
            logging.error("Query: '%s'", query)
            logging.error("Data: '%s'", data)


async def main():
    pr = Processor()
    await pr.connect(source_db, dest_db)
    pr.setup()
    await pr.run(stop_after=1000)

logging.basicConfig(level=logging.DEBUG)
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
