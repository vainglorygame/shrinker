SELECT

id AS "participant_id",
relationships->'player'-> 'data' ->> 'id' AS player_id,
0 AS "rosterId",
0 AS "fk_player_id",
0 AS "fk_match_results_id",
attributes->>'actor' AS "hero",
(attributes->'stats'->>'kills')::int AS "kills",
(attributes->'stats'->>'deaths')::int AS "deaths",
(attributes->'stats'->>'assists')::int AS "assists",
0.0 AS "KD",
0.0 AS "KDA",
0 AS "killParticipation",
(attributes->'stats'->>'nonJungleMinionKills')::int AS "laneMinionsSlayed",
(attributes->'stats'->>'minionKills')::int AS "jungleMinionsSlayed",
(attributes->'stats'->>'turretCaptures')::int AS "turretsDestroyed",
(attributes->'stats'->>'krakenCaptures')::int AS "krakenCaptures",
(attributes->'stats'->>'goldMineCaptures')::int AS "goldMineCaptures",
(attributes->'stats'->>'crystalMineCaptures')::int AS "crystalMineCaptures",
(attributes->'stats'->>'wentAfk')::bool::int AS "wentAfk",
FALSE AS "perfectGame"

FROM participant
