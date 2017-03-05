with stats as (
with plr as (
select 
    match.patch_version as patch_version,
    player.api_id as player_api_id,
    match.game_mode as game_mode,
    participant.winner as winner
from
    match, roster, participant, player
where
    match.api_id = roster.match_api_id and
    roster.api_id = participant.roster_api_id and
    participant.player_api_id = player.api_id and
    player.api_id = $1)
select
    (select distinct(player_api_id) from plr),
    (select distinct(patch_version) from plr),
    (select count(*) as played from plr), 
    (select count(*) as wins from plr where winner = true), 
    (select count(*) as casual_played from plr where game_mode like '%casual%'),
    (select count(*) as casual_wins from plr where game_mode like '%casual%' and winner = true),
    (select count(*) as ranked_played from plr where game_mode like '%ranked%'),
    (select count(*) as ranked_wins from plr where game_mode like '%ranked%' and winner = true),
    (select 0 as streak)
)
INSERT INTO player_stats(player_api_id, patch_version, played, wins, casual_played, casual_wins, ranked_played, ranked_wins, streak)
    SELECT * FROM stats
ON CONFLICT(player_api_id) DO
    UPDATE SET (player_api_id, patch_version, played, wins, casual_played, casual_wins, ranked_played, ranked_wins, streak) = (SELECT * FROM stats)
