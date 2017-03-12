WITH rosters AS (SELECT
  match.data->'data'->'attributes'->>'shardId' AS shardid,
  (match.data->'data'->'attributes'->>'createdAt')::timestamp AS matchdate,
  JSONB_ARRAY_ELEMENTS(match.data->'relations') AS roster
FROM match WHERE id=$1),
participants AS (SELECT
  shardid,
  matchdate,
  JSONB_ARRAY_ELEMENTS(rosters.roster->'relations') AS participant
FROM rosters),
players AS (SELECT
  shardid,
  matchdate,
  JSONB_ARRAY_ELEMENTS(participants.participant->'relations') AS player
FROM participants)

SELECT

players.player->'data'->>'id' AS "api_id",
shardid AS "shard_id",
players.player->'data'->'attributes'->>'name' AS "name",
(players.player->'data'->'attributes'->'stats'->>'level')::int AS "level",
(players.player->'data'->'attributes'->'stats'->>'xp')::float::int AS "xp",
(players.player->'data'->'attributes'->'stats'->>'played')::int AS "played",
(players.player->'data'->'attributes'->'stats'->>'played_ranked')::int AS "played_ranked",
(players.player->'data'->'attributes'->'stats'->>'played')::int - (players.player->'data'->'attributes'->'stats'->>'played_ranked')::int AS "played_casual",
(players.player->'data'->'attributes'->'stats'->>'wins')::int AS "wins",
(players.player->'data'->'attributes'->'stats'->>'lifetimeGold')::float AS "lifetime_gold",
matchdate AS "last_match_created_date",

0 AS "streak"

FROM players
