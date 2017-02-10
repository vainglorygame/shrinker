#!/usr/bin/python

import os
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


class Database(object):
    async def connect(self, **connect_kwargs):
        """Connect to database by arguments."""
        self._pool = await asyncpg.create_pool(**connect_kwargs)

    async def each(self, fetchquery, func, **func_args):
        """Execute a function for each row that is fetched with query."""
        # TODO use iterator instead of callback
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                tasks = []
                async for record in conn.cursor(fetchquery):
                    tasks.append(
                        asyncio.ensure_future(func(record, **func_args))
                    )
                await asyncio.gather(*tasks)

    async def into(self, data, table):
        """Insert a named tuple into a database."""
        items = list(data.items())
        keys, values = [x[0] for x in items], [x[1] for x in items]
        placeholders = ["${}".format(i) for i, _ in enumerate(values, 1)]
        query = "INSERT INTO {} (\"{}\") VALUES ({})".format(
            table, "\", \"".join(keys), ", ".join(placeholders))

        async with self._pool.acquire() as conn:
            await conn.fetch(query, (*data))


async def process():
    sdb = Database()
    await sdb.connect(**source_db)
    ddb = Database()
    await ddb.connect(**dest_db)

    selqry = """
SELECT

(data->'attributes'->>'duration')::int AS "duration",
data->'attributes'->>'gameMode' AS "gameMode",
COALESCE(NULLIF(data->'attributes'->>'patchVersion', ''), '0')::int AS "patchVersion",
data->'attributes'->>'shardId' AS "shard",
data->'attributes'->'stats'->>'endGameReason' AS "result",
data->'attributes'->'stats'->>'queue' AS "queue",

false AS "anyAFK",
'' AS "winningTeam",
0 AS "krakenCaptures",
0 AS "laneMinionsSlayed",
0 AS "jungleMinionsSlayed",
0 AS "heroDeaths"

FROM match LIMIT 100
    """

    await sdb.each(selqry, ddb.into, table="match")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        process()
    )
