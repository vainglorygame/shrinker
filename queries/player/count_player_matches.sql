with stats as (
with plr as (
select 
    'a'::text as "patchVersion",
    player."apiId" as player_api_id,
    match."gameMode" as game_mode,
    participant."winner" as winner
from
    match, roster, participant, player
where
    match."apiId" = roster."match_apiId" and
    roster."apiId" = participant."roster_apiId" and
    participant."player_apiId" = player."apiId" and
    player."apiId" = $1)
select
    (select distinct(player_api_id) from plr),
    (select distinct("patchVersion") as "patchVersion" from plr),
    (select count(*) as played from plr), 
    (select count(*) as wins from plr where winner = true), 
    (select count(*) as casual_played from plr where game_mode like '%casual%'),
    (select count(*) as casual_wins from plr where game_mode like '%casual%' and winner = true),
    (select count(*) as ranked_played from plr where game_mode like '%ranked%'),
    (select count(*) as ranked_wins from plr where game_mode like '%ranked%' and winner = true),
    (select 0 as streak)
)
INSERT INTO player_stats("player_apiId", "patchVersion", played, wins, casual_played, casual_wins, ranked_played, ranked_wins, streak)
    SELECT * FROM stats
