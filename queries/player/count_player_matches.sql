with stats as (
with plr as (
select 
    'a' as patch_version,
    player."apiId" as player_api_id,
    match."gameMode" as game_mode,
    participant."winner" as winner
from
    match, roster, participant, player
where
    match."apiId" = roster."match_apiId" and
    roster."apiId" = participant."roster_apiId" and
    participant."player_apiId" = player."apiId" and
    player."apiId" = '6eab29f6-eed5-11e6-ba5f-068789513eb5')
select
    (select distinct(player_api_id) from plr),
    (select count(*) as played from plr), 
    (select count(*) as wins from plr where winner = true), 
    (select count(*) as casual_played from plr where game_mode like '%casual%'),
    (select count(*) as casual_wins from plr where game_mode like '%casual%' and winner = true),
    (select count(*) as ranked_played from plr where game_mode like '%ranked%'),
    (select count(*) as ranked_wins from plr where game_mode like '%ranked%' and winner = true)
)
INSERT INTO player_stats("player_apiId", played, wins, casual_played, casual_wins, ranked_played, ranked_wins)
    SELECT * FROM stats
