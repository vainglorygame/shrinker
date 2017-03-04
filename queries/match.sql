SELECT

match.id AS "api_id",
(match.data->'data'->'attributes'->>'createdAt')::timestamp as "created_at",
(match.data->'data'->'attributes'->>'duration')::int AS "duration",
match.data->'data'->'attributes'->>'gameMode' AS "game_mode",
COALESCE(NULLIF(match.data->'data'->'attributes'->>'patch_version', '2.2'), '0') AS "patch_version",
match.data->'data'->'attributes'->>'shardId' AS "shard_id",
match.data->'data'->'attributes'->'stats'->>'endGameReason' AS "end_game_reason",
match.data->'data'->'attributes'->'stats'->>'queue' AS "queue",

match.data->'relations'->0->'data'->>'id' as "roster_1",
match.data->'relations'->1->'data'->>'id' as "roster_2"

FROM match WHERE id=$1
