WITH rosters AS (SELECT
  JSONB_ARRAY_ELEMENTS(match.data->'relations') AS roster
FROM match WHERE id=$1),
participants AS (SELECT
  rosters.roster->'data'->>'id' AS rosterid,
  JSONB_ARRAY_ELEMENTS(rosters.roster->'relations') AS participant
FROM rosters)
SELECT

participant->'data'->>'id' AS "apiId",
rosterid AS "roster_apiId",
participant->'relations'->0->'data'->>'id' AS "player_apiId",

participant->'data'->'attributes'->>'actor' AS "hero",
(participant->'data'->'attributes'->'stats'->>'assists')::int AS "assists",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'crystalMineCaptures', ''), '0')::int AS "crystalMineCaptures",
(participant->'data'->'attributes'->'stats'->>'deaths')::int AS "deaths",
(participant->'data'->'attributes'->'stats'->>'farm')::float AS "farm",
(participant->'data'->'attributes'->'stats'->>'firstAfkTime')::float AS "firstAfkTime",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'goldMineCaptures', ''), '0')::int AS "goldMineCaptures",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'jungleKills', ''), '0')::int AS "jungleKills",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'karmaLevel', ''), '0')::int AS "karmaLevel",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'kills', ''), '0')::int AS "kills",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'krakenCaptures', ''), '0')::int AS "krakenCaptures",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'level', ''), '0')::int AS "level",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'minionKills', ''), '0')::int AS "minionKills",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'skillTier', ''), '0')::int AS "skillTier",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'skinKey', ''), '0') AS "skinKey",
COALESCE(NULLIF(participant->'data'->'attributes'->'stats'->>'turretKills', ''), '0')::int::bool AS "turretKills",
(participant->'data'->'attributes'->'stats'->>'wentAfk')::bool AS "wentAfk",
(participant->'data'->'attributes'->'stats'->>'winner')::bool AS "winner",
participant->'data'->'attributes'->'stats'->'itemGrants' AS "itemGrants",
participant->'data'->'attributes'->'stats'->'itemSells' AS "itemSells",
participant->'data'->'attributes'->'stats'->'itemUses' AS "itemUses",
participant->'data'->'attributes'->'stats'->'items' AS "items"

FROM participants
