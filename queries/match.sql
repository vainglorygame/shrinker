SELECT

id AS "apiId",
(attributes->>'createdAt')::timestamp as "createdAt",
(attributes->>'duration')::int AS "duration",
attributes->>'gameMode' AS "gameMode",
COALESCE(NULLIF(attributes->>'patchVersion', ''), '0') AS "patchVersion",
attributes->>'shardId' AS "shardId",
attributes->'stats'->>'endGameReason' AS "endGameReason",
attributes->'stats'->>'queue' AS "queue"

FROM match
