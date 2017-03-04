WITH rosters AS (SELECT
  id AS matchid,
  JSONB_ARRAY_ELEMENTS(match.data->'relations') AS roster
FROM match WHERE id=$1)
SELECT
roster->'data'->>'id' AS "api_id",
matchid AS "match_api_id",
(roster->'data'->'attributes'->'stats'->>'acesEarned')::int AS "aces_earned",
(roster->'data'->'attributes'->'stats'->>'gold')::int AS "gold",
(roster->'data'->'attributes'->'stats'->>'heroKills')::int AS "hero_kills",
(roster->'data'->'attributes'->'stats'->>'krakenCaptures')::int AS "kraken_captures",
roster->'data'->'attributes'->'stats'->>'side' AS "side",
roster->'data'->'attributes'->'stats'->>'side' AS "team_color",
(roster->'data'->'attributes'->'stats'->>'turretKills')::int AS "turret_kills",
(roster->'data'->'attributes'->'stats'->>'turretsRemaining')::int AS "turrets_remaining",

roster->'relations'->0->'data'->>'id' AS "participant_1",
roster->'relations'->1->'data'->>'id' AS "participant_2",
roster->'relations'->2->'data'->>'id' AS "participant_3"
FROM rosters
