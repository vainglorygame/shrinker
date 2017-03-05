WITH rosters AS (SELECT
  (match.data->'data'->'attributes'->>'createdAt')::timestamp AS matchdate,
  JSONB_ARRAY_ELEMENTS(match.data->'relations') AS roster
FROM match WHERE id=$1),
participants AS (SELECT
  matchdate,
  rosters.roster->'data'->>'id' AS rosterid,
  JSONB_ARRAY_ELEMENTS(rosters.roster->'relations') AS participant
FROM rosters)
SELECT

participant->'data'->>'id' AS "api_id",
rosterid AS "roster_api_id",
participant->'relations'->0->'data'->>'id' AS "player_api_id",
matchdate AS "created_at",

participant->'data'->'attributes'->>'actor' AS "hero",
(participant->'data'->'attributes'->'stats'->>'assists')::int AS "assists",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'crystalMineCaptures', ''), '0')::int AS "crystal_mine_captures",
(participant->'data'->'attributes'->'stats'->>'deaths')::int AS "deaths",
(participant->'data'->'attributes'->'stats'->>'farm')::float AS "farm",
(participant->'data'->'attributes'->'stats'->>'firstAfkTime')::float AS "first_afk_time",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'goldMineCaptures', ''), '0')::int AS "gold_mine_captures",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'jungleKills', ''), '0')::int AS "jungle_kills",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'karmaLevel', ''), '0')::int AS "karma_level",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'kills', ''), '0')::int AS "kills",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'krakenCaptures', ''), '0')::int AS "kraken_captures",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'level', ''), '0')::int AS "level",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'minionKills', ''), '0')::int AS "minion_kills",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'skillTier', ''), '0')::int AS "skill_tier",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'skinKey', ''), '0') AS "skin_key",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'turretKills', ''), '0')::int AS "turret_kills",
(participant->'data'->'attributes'->'stats'->>'wentAfk')::bool AS "went_afk",
(participant->'data'->'attributes'->'stats'->>'winner')::bool AS "winner",
participant->'data'->'attributes'->'stats'->'itemGrants' AS "item_grants",
participant->'data'->'attributes'->'stats'->'itemSells' AS "item_sells",
participant->'data'->'attributes'->'stats'->'itemUses' AS "item_uses",
participant->'data'->'attributes'->'stats'->'items' AS "items"

FROM participants
