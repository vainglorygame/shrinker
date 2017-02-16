SELECT

attributes->'stats'->>'side' AS "team",
FALSE AS "winner",
FALSE AS "surrender",
0 AS "teamSize",
'' AS "hero_1",
'' AS "hero_2",
'' AS "hero_3",
0 AS "laneMinionsSlayed",
0 AS "jungleMinionsSlayed",
(attributes->'stats'->>'turretKills')::int AS "turretsDestroyed",
(attributes->'stats'->>'heroKills')::int AS "kills",
0 AS "assists",
0 AS "deaths",
(attributes->'stats'->>'krakenCaptures')::int AS "krakenCaptures",
0 AS "goldMineCaptures",
0 AS "crystalMineCaptures",
0 AS "afkCount",
0 AS "afkTime"

FROM apidata WHERE type='roster'
