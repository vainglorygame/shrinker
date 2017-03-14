WITH stats AS (
    SELECT
        api_id,
        CASE WHEN roster.hero_kills=0
        THEN 0
        ELSE (kills+assists)/roster.hero_kills::float
        END AS kill_participation
    FROM participant JOIN roster ON roster.api_id=participant.roster_api_id
)
INSERT INTO participant_stats(participant_api_id, kill_participation)
SELECT * FROM stats
ON CONFLICT(participant_api_id) DO
UPDATE SET(participant_api_id, kill_participation) = (SELECT * FROM stats)
