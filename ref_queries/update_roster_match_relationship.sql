-- i am thinking how fast slow this will be and if we should do this in DB.
-- maybe we need a script to run a cursor and update records - dont know...
-- a few runs on 35K records was closing my server connection on my shitty laptop

-- this will update all roster with match_apiId
update "roster" 
set "match_apiId" = match."apiId"
from match
where (roster."apiId" = roster_1 or roster."apiId" = roster_2)


-- this is same as before however should only pick empty records
update "roster" 
set "match_apiId" = match."apiId"
from match
where roster."match_apiId" is not null and (roster."apiId" = roster_1 or roster."apiId" = roster_2)



-- this should always return a count of 2 for each match_apiId :)
select "match_apiId", count(*) from roster group by "match_apiId"
