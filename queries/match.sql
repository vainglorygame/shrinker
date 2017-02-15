SELECT

id AS "match_id",
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
