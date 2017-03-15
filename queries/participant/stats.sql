WITH stats AS (
    SELECT
        participant.api_id,
        match.patch_version,
        CASE WHEN roster.hero_kills=0
        THEN 0
        ELSE (kills+assists)/roster.hero_kills::float
        END AS kills_participation,
        CASE WHEN participant.deaths=0
        THEN 0
        ELSE (kills+assists)/deaths::float
        END AS kda
    FROM participant
    JOIN roster ON roster.api_id=participant.roster_api_id
    JOIN match ON match.api_id=roster.match_api_id
    WHERE participant.api_id = $1
)
INSERT INTO participant_stats(participant_api_id, patch_version, kills_participation, kda)
SELECT * FROM stats
ON CONFLICT(participant_api_id) DO
UPDATE SET(participant_api_id, patch_version, kills_participation, kda) = (SELECT * FROM stats)
