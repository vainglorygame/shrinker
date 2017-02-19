SELECT

id AS "apiId",
(attributes->'stats'->>'acesEarned')::int AS "acesEarned",
(attributes->'stats'->>'gold')::int AS "gold",
(attributes->'stats'->>'heroKills')::int AS "heroKills",
(attributes->'stats'->>'krakenCaptures')::int AS "krakenCaptures",
attributes->'stats'->>'side' AS "side",
attributes->'stats'->>'side' AS "teamColor",
(attributes->'stats'->>'turretKills')::int AS "turretKills",
(attributes->'stats'->>'turretsRemaining')::int AS "turretsRemaining",

relationships->'participants'->'data'->0->>'id' AS "participant_1",
relationships->'participants'->'data'->1->>'id' AS "participant_2",
relationships->'participants'->'data'->2->>'id' AS "participant_3"

FROM roster
