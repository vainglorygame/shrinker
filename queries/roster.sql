WITH rosters AS (SELECT
  id AS matchid,
  JSONB_ARRAY_ELEMENTS(match.data->'relations') AS roster
FROM match WHERE id=$1)
SELECT
roster->'data'->>'id' AS "apiId",
matchid AS "match_apiId",
(roster->'data'->'attributes'->'stats'->>'acesEarned')::int AS "acesEarned",
(roster->'data'->'attributes'->'stats'->>'gold')::int AS "gold",
(roster->'data'->'attributes'->'stats'->>'heroKills')::int AS "heroKills",
(roster->'data'->'attributes'->'stats'->>'krakenCaptures')::int AS "krakenCaptures",
roster->'data'->'attributes'->'stats'->>'side' AS "side",
roster->'data'->'attributes'->'stats'->>'side' AS "teamColor",
(roster->'data'->'attributes'->'stats'->>'turretKills')::int AS "turretKills",
(roster->'data'->'attributes'->'stats'->>'turretsRemaining')::int AS "turretsRemaining",

roster->'relations'->0->'data'->>'id' AS "participant_1",
roster->'relations'->1->'data'->>'id' AS "participant_2",
roster->'relations'->2->'data'->>'id' AS "participant_3"
FROM rosters
