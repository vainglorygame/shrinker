WITH rosters AS (SELECT
  match.data->'data'->'attributes'->>'createdAt' AS matchdate,
  JSONB_ARRAY_ELEMENTS(match.data->'relations') AS roster
FROM match WHERE id=$1),
participants AS (SELECT
  matchdate,
  JSONB_ARRAY_ELEMENTS(rosters.roster->'relations') AS participant
FROM rosters),
players AS (SELECT
  matchdate,
  JSONB_ARRAY_ELEMENTS(participants.participant->'relations') AS player
FROM participants)

SELECT

players.player->'data'->>'id' AS "apiId",
players.player->'data'->'attributes'->>'name' AS "name",
(players.player->'data'->'attributes'->'stats'->>'level')::int AS "level",
(players.player->'data'->'attributes'->'stats'->>'xp')::int AS "xp",
(players.player->'data'->'attributes'->'stats'->>'played')::int AS "played",
(players.player->'data'->'attributes'->'stats'->>'played_ranked')::int AS "playedRanked",
(players.player->'data'->'attributes'->'stats'->>'played')::int - (players.player->'data'->'attributes'->'stats'->>'played_ranked')::int AS "playedCasual",
(players.player->'data'->'attributes'->'stats'->>'wins')::int AS "wins",
(players.player->'data'->'attributes'->'stats'->>'lifetimeGold')::float AS "lifetimeGold",
CASE WHEN players.player->'data'->'attributes'->>'name'=$2 THEN matchdate ELSE 'epoch'::timestamp::text END AS "lastMatchCreatedDate",

0 AS "streak"

FROM players
