SELECT

id AS "apiId",
(attributes->>'createdAt')::timestamp as "createdAt",
(attributes->>'duration')::int AS "duration",
attributes->>'gameMode' AS "gameMode",
COALESCE(NULLIF(attributes->>'patchVersion', ''), '0') AS "patchVersion",
attributes->>'shardId' AS "shardId",
attributes->'stats'->>'endGameReason' AS "endGameReason",
attributes->'stats'->>'queue' AS "queue" ,

relationships->'rosters'->'data'->0->>'id' as "roster_1",
relationships->'rosters'->'data'->1->>'id' as "roster_2"

FROM "match"
