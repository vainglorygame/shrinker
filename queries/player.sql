-- count the number of matches we have from that player in the database
-- UPDATE player SET "games_in_db"=
SELECT
COUNT(DISTINCT match."apiId") AS "games_in_db"
FROM player
JOIN participant ON player."apiId" = participant."player_apiId"
JOIN roster ON participant."roster_apiId" = roster."apiId"
JOIN match ON roster."match_apiId" = match."apiId"
WHERE player.id=$1
