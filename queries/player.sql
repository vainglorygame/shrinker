SELECT

id AS "apiId",
attributes->>'name' AS "name",
(attributes->'stats'->>'level')::int AS "level",
(attributes->'stats'->>'xp')::int AS "xp",
(attributes->'stats'->>'played')::int AS "played",
0 AS "totalGamePlaytime",
(attributes->'stats'->>'played_ranked')::int AS "playedRanked",
(attributes->'stats'->>'wins')::int AS "wins",
(attributes->'stats'->>'winStreak')::int AS "streak",
'' AS "herosUnlocked",
'' "skinsUnlocked",
attributes->'stats'->>'lifetimeGold' AS "lifeTimeGold",
0 AS "lifeTimeKills",
0 AS "lifeTimeDeaths",
0 AS "lifeTimeAssists",
0 AS "lifeTimeKD",
0 AS "lifeTimeKDA"

FROM player
