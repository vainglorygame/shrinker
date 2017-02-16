SELECT

relationships->'player'->'data'->>'id' AS player_id,
0 AS "match_id",
0 AS "rosterId",
0 AS "fk_player_id",
0 AS "fk_match_results_id",
attributes->>'actor' AS "hero",
COALESCE(NULLIF(attributes->'stats'->>'kills', ''), '0')::int AS "kills",
(attributes->'stats'->>'deaths')::int AS "deaths",
(attributes->'stats'->>'assists')::int AS "assists",
0.0 AS "KD",
0.0 AS "KDA",
0 AS "killParticipation",
COALESCE(NULLIF(attributes->'stats'->>'nonJungleMinionKills', ''), '0')::int AS "laneMinionsSlayed",
COALESCE(NULLIF(attributes->'stats'->>'minionKills', ''), '0')::int AS "jungleMinionsSlayed",
COALESCE(NULLIF(attributes->'stats'->>'turretCaptures', ''), '0')::int AS "turretsDestroyed",
COALESCE(NULLIF(attributes->'stats'->>'krakenCaptures', ''), '0')::int AS "krakenCaptures",
COALESCE(NULLIF(attributes->'stats'->>'goldMineCaptures', ''), '0')::int AS "goldMineCaptures",
COALESCE(NULLIF(attributes->'stats'->>'crystalMineCaptures', ''), '0')::int AS "crystalMineCaptures",
(attributes->'stats'->>'wentAfk')::bool::int AS "wentAfk",
FALSE AS "perfectGame"

FROM participant
