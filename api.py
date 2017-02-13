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


class Processor(object):
    def __init__(self):
        self._queries = {}
        self._srcpool = self._destpool = None

    def setup(self):
        """Load conversion queries."""
        self._queries = {
            "match": """
SELECT

(attributes->>'duration')::int AS "duration",
attributes->>'gameMode' AS "gameMode",
COALESCE(NULLIF(attributes->>'patchVersion', ''), '0')::int AS "patchVersion",
attributes->>'shardId' AS "shard",
attributes->'stats'->>'endGameReason' AS "result",
attributes->'stats'->>'queue' AS "queue",

false AS "anyAFK",
'' AS "winningTeam",
0 AS "krakenCaptures",
0 AS "laneMinionsSlayed",
0 AS "jungleMinionsSlayed",
0 AS "heroDeaths"

FROM apidata WHERE type='match'
        """,
            "match_results": """
SELECT

attributes->'stats'->>'side' AS "team",
FALSE AS "winner",
FALSE AS "surrender",
0 AS "teamSize",
'' AS "hero_1",
'' AS "hero_2",
'' AS "hero_3",
0 AS "laneMinionsSlayed",
0 AS "jungleMinionsSlayed",
0 AS "turretsDestroyed",
(attributes->'stats'->>'heroKills')::int AS "heroKills",
0 AS "heroAssits",
0 AS "heroDeaths",
(attributes->'stats'->>'krakenCaptures')::int AS "krakensCaptured",
0 AS "goldMineCaptures",
0 AS "crystalMineCaptures",
0 AS "afkCount",
0 AS "afkTime"

FROM apidata WHERE type='roster'
        """,
            "match_participation": """
SELECT

0 AS "rosterId",
attributes->>'actor' AS "hero",
(attributes->'stats'->>'kills')::int AS "kills",
(attributes->'stats'->>'deaths')::int AS "deaths",
(attributes->'stats'->>'assists')::int AS "assists",
0.0 AS "kd",
0.0 AS "kda",
(attributes->'stats'->>'nonJungleMinionKills')::int AS "laneMinionsSlayed",
(attributes->'stats'->>'minionKills')::int AS "jungleMinionsSlayed",
(attributes->'stats'->>'turretCaptures')::int AS "turretsDestroyed",
0 AS "heroKills",
0 AS "heroDeaths",
0 AS "heroAssits",
(attributes->'stats'->>'krakenCaptures')::int AS "krakensCaptured",
(attributes->'stats'->>'goldMineCaptures')::int AS "goldMineCaptures",
(attributes->'stats'->>'crystalMineCaptures')::int AS "crystalMineCaptures",
(attributes->'stats'->>'wentAfk')::bool::int AS "afkCount",
(attributes->'stats'->>'firstAfkTime')::float::int AS "afkTime",
FALSE AS "perfectGame"

FROM apidata WHERE type='participant'
        """,
            "player": """
SELECT

id AS "apiId",
attributes->>'name' AS "name",
(attributes->'stats'->>'level')::int AS "level",
(attributes->'stats'->>'xp')::int AS "xp",
(attributes->'stats'->>'played')::int AS "played",
(attributes->'stats'->>'played_ranked')::int AS "playedRanked",
(attributes->'stats'->>'wins')::int AS "wins",
(attributes->'stats'->>'winStreak')::int AS "streak",
'' AS "herosUnlocked",
'' "skinsUnlocked",
0 AS "totalGamePlaytime",
attributes->'stats'->>'lifetimeGold' AS "lifeTimeGold",
0 AS "lifeTimeKills",
0 AS "lifeTimeDeaths",
0 AS "lifeTimeAssists",
'' AS "bestHeroPlayingWith",
'' AS "worstHeroPlayingWith",
'' AS "bestHeroPlayingAgainst",
'' AS "worstHeroPlayingAgainst",
0 AS "lifeTimeKD",
0 AS "lifeTimeKDA",
'[]'::jsonb AS "heroPerformance",
'[]'::jsonb AS "rolePerformance"

FROM apidata WHERE type='player'
        """
        }

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
                    stop_after -= 1  # TODO for debugging, don't process whole table
                    logging.info("processing table %s", table)
                    async with srccon.transaction():
                        async with destcon.transaction():
                            async for rec in srccon.cursor(query):
                                await self.into(destcon, rec, table)

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
    pr.setup()
    await pr.run(stop_after=1000)

logging.basicConfig(level=logging.DEBUG)
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
