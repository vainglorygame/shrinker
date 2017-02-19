SELECT

id AS "apiId",
attributes->>'name' AS "name",
(attributes->'stats'->>'level')::int AS "level",
(attributes->'stats'->>'xp')::int AS "xp",
(attributes->'stats'->>'played')::int AS "played",
(attributes->'stats'->>'played_ranked')::int AS "playedRanked",
(attributes->'stats'->>'played')::int - (attributes->'stats'->>'played_ranked')::int AS "playedCasual",
(attributes->'stats'->>'wins')::int AS "wins",
(attributes->'stats'->>'lifetimeGold')::float AS "lifetimeGold",

0 AS "streak"

FROM player
