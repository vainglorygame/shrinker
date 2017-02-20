SELECT

id AS "apiId",
relationships->'player'->'data'->>'id' AS "player_apiId",

attributes->>'actor' AS "hero",
(attributes->'stats'->>'assists')::int AS "assists",
COALESCE(NULLIF(attributes->'stats'->>'crystalMineCaptures', ''), '0')::int AS "crystalMineCaptures",
(attributes->'stats'->>'deaths')::int AS "deaths",
(attributes->'stats'->>'farm')::float AS "farm",
(attributes->'stats'->>'firstAfkTime')::float AS "firstAfkTime",
COALESCE(NULLIF(attributes->'stats'->>'goldMineCaptures', ''), '0')::int AS "goldMineCaptures",
COALESCE(NULLIF(attributes->'stats'->>'jungleKills', ''), '0')::int AS "jungleKills",
COALESCE(NULLIF(attributes->'stats'->>'karmaLevel', ''), '0')::int AS "karmaLevel",
COALESCE(NULLIF(attributes->'stats'->>'kills', ''), '0')::int AS "kills",
COALESCE(NULLIF(attributes->'stats'->>'krakenCaptures', ''), '0')::int AS "krakenCaptures",
COALESCE(NULLIF(attributes->'stats'->>'level', ''), '0')::int AS "level",
COALESCE(NULLIF(attributes->'stats'->>'minionKills', ''), '0')::int AS "minionKills",
COALESCE(NULLIF(attributes->'stats'->>'skillTier', ''), '0')::int AS "skillTier",
COALESCE(NULLIF(attributes->'stats'->>'skinKey', ''), '0') AS "skinKey",
(attributes->'stats'->>'wentAfk')::bool::bool AS "wentAfk",
(attributes->'stats'->>'winner')::bool::bool AS "winner",
attributes->'stats'->'itemGrants' AS "itemGrants",
attributes->'stats'->'itemSells' AS "itemSells",
attributes->'stats'->'itemUses' AS "itemUses",
attributes->'stats'->'items' AS "items"

FROM "participant"
