-- UNTESTED!
WITH plr AS (
    SELECT
        "id",
        "apiId",
        -- count matches in db
        COUNT(DISTINCT participant."apiId") AS "games_in_db"
    FROM player
    JOIN participant ON player."apiId" = participant."player_apiId"
    JOIN roster ON participant."roster_apiId" = roster."apiId"
    WHERE player."apiId"=$1
)
INSERT INTO player_stats("id", "apiId", "games_in_db")
    SELECT * FROM plr
ON CONFLICT("apiId") DO
UPDATE SET "games_in_db"=plr."games_in_db"
