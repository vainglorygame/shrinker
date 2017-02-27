SELECT

match.id AS "apiId",
(match.data->'data'->'attributes'->>'createdAt')::timestamp as "createdAt",
(match.data->'data'->'attributes'->>'duration')::int AS "duration",
match.data->'data'->'attributes'->>'gameMode' AS "gameMode",
COALESCE(NULLIF(match.data->'data'->'attributes'->>'patchVersion', ''), '0') AS "patchVersion",
match.data->'data'->'attributes'->>'shardId' AS "shardId",
match.data->'data'->'attributes'->'stats'->>'endGameReason' AS "endGameReason",
match.data->'data'->'attributes'->'stats'->>'queue' AS "queue",

match.data->'relations'->0->'data'->>'id' as "roster_1",
match.data->'relations'->1->'data'->>'id' as "roster_2"

FROM match WHERE id=$1
