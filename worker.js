#!/usr/bin/node
/* jshint esnext:true */
"use strict";

const amqp = require("amqplib"),
    winston = require("winston"),
    loggly = require("winston-loggly-bulk"),
    Seq = require("sequelize"),
    item_name_map = require("../orm/items"),
    hero_name_map = require("../orm/heroes"),
    Promise = require("bluebird"),
    sleep = require("sleep-promise");

const RABBITMQ_URI = process.env.RABBITMQ_URI,
    DATABASE_URI = process.env.DATABASE_URI,
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN,
    // matches + players, 5 players with 50 matches as default
    BATCHSIZE = parseInt(process.env.BATCHSIZE) || 5 * (50 + 1),
    // maximum number of elements to be inserted in one statement
    CHUNKSIZE = parseInt(process.env.CHUNKSIZE) || 100,
    LOAD_TIMEOUT = parseFloat(process.env.LOAD_TIMEOUT) || 5000, // ms
    IDLE_TIMEOUT = parseFloat(process.env.IDLE_TIMEOUT) || 700;  // ms

const logger = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({
            timestamp: true,
            colorize: true
        })
    ]
});

// loggly integration
if (LOGGLY_TOKEN)
    logger.add(winston.transports.Loggly, {
        inputToken: LOGGLY_TOKEN,
        subdomain: "kvahuja",
        tags: ["backend", "processor"],
        json: true
    });

// helpers
const camelCaseRegExp = new RegExp(/([a-z])([A-Z]+)/g);
function camelToSnake(text) {
    return text.replace(camelCaseRegExp, (m, $1, $2) =>
        $1 + "_" + $2.toLowerCase());
}

function snakeCaseKeys(obj) {
    Object.keys(obj).forEach((key) => {
        const new_key = camelToSnake(key);
        if (new_key == key) return;
        obj[new_key] = obj[key];
        delete obj[key];
    });
    return obj;
}

// split an array into arrays of max chunksize
function* chunks(arr) {
    for (let c=0, len=arr.length; c<len; c+=CHUNKSIZE)
        yield arr.slice(c, c+CHUNKSIZE);
}

// helper to convert API response into flat JSON
// db structure is (almost) 1:1 the API structure
// so we can insert the flat API response as-is
function flatten(obj) {
    const attrs = obj.attributes || {},
        stats = attrs.stats || {};
    let o = Object.assign({}, obj, attrs, stats);
    o.api_id = o.id;  // rename
    delete o.id;
    delete o.type;
    delete o.attributes;
    delete o.stats;
    delete o.relationships;
    return snakeCaseKeys(o);
}

// main code
(async () => {
    let seq, model, rabbit, ch;

    while (true) {
        try {
            seq = new Seq(DATABASE_URI, {
                logging: false,
                max: 10
            });
            rabbit = await amqp.connect(RABBITMQ_URI, { heartbeat: 120 });
            ch = await rabbit.createChannel();
            await ch.assertQueue("process", {durable: true});
            break;
        } catch (err) {
            logger.error("Error connecting", err);
            await sleep(5000);
        }
    }

    model = require("../orm/model")(seq, Seq);

    let load_timer = undefined,
        idle_timer = undefined,
        profiler = undefined;

    let item_db_map = new Map(),      // "Halcyon Potion" to id
        hero_db_map = new Map(),      // "*SAW*" to id
        series_db_map = new Map(),    // date to series id
        game_mode_db_map = new Map(), // "ranked" to id
        role_db_map = new Map(),      // "captain" to id
        hero_role_map = new Map();    // SAW.id to "carry"

    await Promise.all([
        model.Item.findAll()
            .map((item) => item_db_map[item.name] = item.id),
        model.Hero.findAll()
            .map((hero) => {
                hero_db_map[hero.name] = hero.id;
                if (hero.is_carry)
                    hero_role_map[hero.id] = "carry";
                if (hero.is_jungler)
                    hero_role_map[hero.id] = "jungler";
                if (hero.is_captain)
                    hero_role_map[hero.id] = "captain";
            }),
        model.Series.findAll()
            .map((series) => {
                if (series.dimension_on == "player")
                    series_db_map[series.name] = series.id;
            }),
        model.GameMode.findAll()
            .map((mode) => game_mode_db_map[mode.name] = mode.id),
        model.Role.findAll()
            .map((role) => role_db_map[role.name] = role.id)
    ]);

    // as long as the queue is filled, msg are not ACKed
    // server sends as long as there are less than `prefetch` unACKed
    await ch.prefetch(BATCHSIZE);

    let player_data = new Set(),
        match_data = new Set(),
        msg_buffer = new Set();

    ch.consume("process", async (msg) => {
        if (msg.properties.type == "player") {
            // apigrabber sends an array
            player_data.add(...JSON.parse(msg.content));
            msg_buffer.add(msg);
        }
        if (msg.properties.type == "match") {
            // apigrabber sends a single object
            const match = JSON.parse(msg.content);
            if (await model.Match.count({
                where: { api_id: match.id }
            }) > 0) {
                await ch.nack(msg, false, false);  // discard
                return;
            } else
                match_data.add(match);
            msg_buffer.add(msg);
        }

        // fill queue until batchsize or idle
        // for logging of the time between batch fill and batch process
        if (profiler == undefined) profiler = logger.startTimer();
        // timeout after first job
        if (load_timer == undefined)
            load_timer = setTimeout(process, LOAD_TIMEOUT);
        // timeout after last job
        if (idle_timer != undefined)
            clearTimeout(idle_timer);
        idle_timer = setTimeout(process, IDLE_TIMEOUT);
        // maximum data pressure
        if (match_data.size + player_data.size == BATCHSIZE)
            await process();
    }, { noAck: false });

    async function process() {
        profiler.done("buffer filled");
        profiler = undefined;

        logger.info("processing batch",
            { players: player_data.size, matches: match_data.size });

        // clean up to allow processor to accept while we wait for db
        clearTimeout(idle_timer);
        clearTimeout(load_timer);
        const player_objects = new Set(player_data),
            match_objects = new Set(match_data),
            msgs = new Set(msg_buffer);
        idle_timer = undefined;
        load_timer = undefined;
        player_data.clear();
        match_data.clear();
        msg_buffer.clear();

        let processed_players = new Set(), // to sort out duplicates
            notify_players_matches = new Set(),  // players with new matches
            notify_players_stats = new Set();  // players with new stats

        // aggregate record objects to do a bulk insert
        let match_records = [],
            roster_records = [],
            participant_records = [],
            participant_stats_records = [],
            player_records = [],
            player_records_direct = [],  // via `/players`
            asset_records = [],
            participant_item_records = [];

        // populate `_records`
        // data from `/players`
        // `each` executes serially so there are
        // no race conditions within one batch
        await Promise.each(player_objects, async (p) => {
            const player = flatten(p);
            if (processed_players.has(player.api_id)) {
                logger.warn("got player in additional region",
                    { name: player.name, region: player.shard_id });
                // see below, this is handling region changes
                // when player objects end up in the same batch
                const duplicate = player_records_direct.find(
                    (pr) => pr.api_id == player.api_id);
                if (duplicate) {
                    console.error("duplicate disappeared! please investigate, ignoring for now…");
                    return;
                }
                if (duplicate.created_at < player.created_at) {
                    // replace by newer one as below
                    player_records_direct.splice(
                        player_records_direct.indexOf(duplicate), 1);
                } else {
                    logger.warn("ignoring player from additional region",
                        { name: player.name, region: player.shard_id });
                    return;
                }
            } else {
                processed_players.add(player.api_id);
            }
            // player objects that arrive here came from a search
            // with search, updater can't update last_update
            player.last_update = seq.fn("NOW");
            logger.info("processing player",
                { name: player.name, region: player.shard_id });
            // check whether there is a player in db
            // that has a more recent `created_at`
            // this is only the case with region changes
            const count = await model.Player.count({
                where: {
                    api_id: player.api_id,
                    created_at: {
                        // equal: just update last_update
                        $gt: player.created_at
                    }
                }});
            if (count > 0) {
                logger.warn("ignoring player who seems to have switched from region",
                    { name: player.name, region: player.shard_id });
                return;
            } else {
                player_records_direct.push(player);
                notify_players_stats.add(player.name);
            }
        });

        // reject invalid matches (handling API bugs)
        match_objects.forEach((match, idx) => {
            if (match.rosters[0].id == "null")
                delete match_objects[idx];
        });

        // data from `/matches`
        match_objects.forEach((match) => {
            // flatten jsonapi nested response into our db structure-like shape
            // also, push missing fields
            match.rosters = match.rosters.map((roster) => {
                roster.matchApiId = match.id;
                roster.attributes.shardId = match.attributes.shardId;
                roster.createdAt = match.createdAt;
                // TODO API workaround: roster does not have `winner`
                if (roster.participants.length > 0)
                    roster.attributes.stats.winner = roster.participants[0].stats.winner;
                else  // Blitz 2v0, see 095e86e4-1bd3-11e7-b0b1-0297c91b7699 on eu
                    roster.attributes.stats.winner = false;

                roster.participants = roster.participants.map((participant) => {
                    // ! attributes added here need to be added via `calculate_participant_stats` too
                    participant.attributes.shardId = roster.attributes.shardId;
                    participant.rosterApiId = roster.id;
                    participant.matchApiId = match.id;
                    participant.createdAt = roster.createdAt;
                    participant.playerApiId = participant.player.id;

                    // API bug fixes (TODO)
                    // items on AFK is `null` not `{}`
                    participant.attributes.stats.itemGrants = participant.attributes.stats.itemGrants || {};
                    participant.attributes.stats.itemSells = participant.attributes.stats.itemSells || {};
                    participant.attributes.stats.itemUses = participant.attributes.stats.itemUses || {};
                    // jungle_kills is `null` in BR
                    participant.attributes.stats.jungleKills = participant.attributes.stats.jungleKills || 0;

                    // map items: names/id -> name -> db
                    const item_use = (arr, action) =>
                            arr.map((item, idx) => { return {
                                number: idx,
                                participant_api_id: participant.id,
                                item_id: item_db_map[item_name_map[item]],
                                action: action
                            } }),
                        item_arr_from_obj = (obj) =>
                            [].concat(...  // 3 flatten
                                Object.entries(obj).map(  // 1 map over (key, value)
                                    (tuple) => Array(tuple[1]).fill(tuple[0])))  // 2 create Array [key] * value
                    let itms = [];

                    itms = itms.concat(item_use(participant.attributes.stats.items, "final"));
                    itms = itms.concat(item_use(item_arr_from_obj(participant.attributes.stats.itemGrants), "grant"));
                    itms = itms.concat(item_use(item_arr_from_obj(participant.attributes.stats.itemUses), "use"));
                    itms = itms.concat(item_use(item_arr_from_obj(participant.attributes.stats.itemSells), "sell"));

                    // for debugging:
                    /*
                    let items_missing_name = [].concat(...
                        Object.keys(participant.attributes.stats.itemGrants),
                        participant.attributes.stats.items)
                        .filter((i) => Object.keys(item_name_map).indexOf(i) == -1);
                    if (items_missing_name.length > 0) console.error("missing API name -> name mapping for", items_missing_name);

                    let items_missing_db = [].concat(...
                        Object.keys(participant.attributes.stats.itemGrants),
                        participant.attributes.stats.items)
                        .filter((i) => Object.keys(item_db_map).indexOf(item_name_map[i]) == -1);
                    if (items_missing_db.length > 0) console.error("missing name -> DB ID mapping for", items_missing_db);
                    */

                    // redefine participant.items for our custom map
                    participant.attributes.stats.participant_items = itms;

                    participant.player.attributes.shardId = participant.attributes.shardId;
                    participant.player = flatten(participant.player);
                    return flatten(participant);
                });
                return flatten(roster);
            });
            match.assets = match.assets.map((asset) => {
                asset.matchApiId = match.id;
                asset.attributes.shardId = match.attributes.shardId;
                return flatten(asset);
            });
            match = flatten(match);

            // after conversion, create the array of records
            // there is a low chance of a match being duplicated in a batch,
            // skip it and its children
            match_records.push(match);
            match.rosters.forEach((r) => {
                roster_records.push(r);
                r.participants.forEach((p) => {
                    const p_pstats = calculate_participant_stats(match, r, p);
                    // participant gets split into participant and p_stats
                    participant_records.push(p_pstats[0]);
                    participant_stats_records.push(p_pstats[1]);
                    // deduplicate player
                    // in a batch, it is very likely that players are duplicated
                    // so this improves performance a bit
                    if (!processed_players.has(p.player.api_id)) {
                        processed_players.add(p.player.api_id);
                        player_records.push(p.player);
                        notify_players_matches.add(p.player.name);
                    }
                    p.participant_items.forEach((i) => {
                        participant_item_records.push(i);
                    });
                });
            });
            match.assets.forEach((a) => {
                asset_records.push(a);
            });
        });

        let transaction_profiler = logger.startTimer();
        // now access db
        try {
            // upsert whole batch in parallel
            await seq.query("SET unique_checks=0");
            logger.info("inserting batch into db");
            await seq.transaction({ autocommit: false }, async (transaction) => {
                await Promise.all([
                    Promise.map(chunks(match_records), async (m_r) =>
                        model.Match.bulkCreate(m_r, {
                            ignoreDuplicates: true,  // should not happen
                            transaction: transaction
                        })
                    ),
                    Promise.map(chunks(roster_records), async (r_r) =>
                        model.Roster.bulkCreate(r_r, {
                            ignoreDuplicates: true,
                            transaction: transaction
                        })
                    ),
                    Promise.map(chunks(participant_records), async (p_r) =>
                        model.Participant.bulkCreate(p_r, {
                            ignoreDuplicates: true,
                            transaction: transaction
                        })
                    ),
                    Promise.map(chunks(participant_stats_records), async (p_s_r) =>
                        model.ParticipantStats.bulkCreate(p_s_r, {
                            ignoreDuplicates: true,
                            transaction: transaction
                        })
                    ),
                    Promise.map(chunks(player_records), async (pl_r) =>
                        model.Player.bulkCreate(pl_r, {
                            ignoreDuplicates: true,
                            transaction: transaction
                        })
                    ),
                    Promise.map(chunks(player_records_direct), async (p_r_d) =>
                        model.Player.bulkCreate(player_records_direct, {
                            // if set to [] (all), upsert messes with autoincrement
                            updateOnDuplicate: [
                                "shard_id", "api_id", "name", "last_update",
                                "created_at", "level", "xp", "lifetime_gold"
                            ],
                            transaction: transaction
                        })
                    ),
                    Promise.map(chunks(participant_item_records), async (p_i_r) =>
                        model.ItemParticipant.bulkCreate(p_i_r, {
                            ignoreDuplicates: true,
                            transaction: transaction
                        })
                    ),
                    Promise.map(chunks(asset_records), async (a_r) =>
                        model.Asset.bulkCreate(a_r, {
                            ignoreDuplicates: true,
                            transaction: transaction
                        })
                    )
                ])
            });
            await seq.query("SET unique_checks=1");

            logger.info("acking batch", { size: msgs.size });
            for (let msg of msgs)
                await ch.ack(msg);
        } catch (err) {
            // this should only happen for Deadlocks in prod
            // it *must not* fail due to broken schema or missing dependency
            // TODO: eliminate such cases earlier in the chain
            // and immediately NACK those broken matches, requeueing only the rest
            logger.error("SQL error", err);
            for (let msg of msgs)
                await ch.nack(msg, false, true);
            return;  // give up
        }
        transaction_profiler.done("database transaction");

        // notify web
        // …about new matches
        for (let name of notify_players_matches)
            await ch.publish("amq.topic", "player." + name, new Buffer("matches_update"));
        // …about updated lifetime stats
        for (let name of notify_players_stats)
            await ch.publish("amq.topic", "player." + name, new Buffer("stats_update"));
        // …global about new matches
        if (match_records.length > 0)
            await ch.publish("amq.topic", "global", new Buffer("matches_update"));
    }

    // Split participant API data into participant and participant_stats
    // Should not need to query db here.
    function calculate_participant_stats(match, roster, participant) {
        let p_s = {},  // participant_stats_record
            p = {};  // participant_record

        // meta
        p_s.participant_api_id = participant.api_id;
        p_s.final = true;  // these are the stats at the end of the match
        p_s.updated_at = new Date();
        p_s.created_at = new Date(Date.parse(match.created_at));
        p_s.created_at.setMinutes(p_s.created_at.getMinutes() + match.duration / 60);
        p.participant_items = participant.participant_items;
        p.created_at = match.created_at;
        // mappings
        // hero names additionally need to be mapped old to new names
        // (Sayoc = Taka)
        p.hero_id = hero_db_map[hero_name_map[participant.actor]];
        // TODO don't hardcode this, waiting for `match.patch_version` to be available
        if (p_s.created_at < new Date("2017-03-28"))
            p.series_id = series_db_map["Patch 2.2"];
        else
            p.series_id = series_db_map["Patch 2.3"];
        p.game_mode_id = game_mode_db_map[match.game_mode];
        p.role_id = role_db_map[classify_role(p)];

        // attributes to copy from API to participant
        // these don't change over the duration of the match
        // (or aren't in Telemetry)
        ["api_id", "shard_id", "player_api_id", "roster_api_id", "match_api_id",
            "winner", "went_afk", "first_afk_time",
            "skin_key", "skill_tier", "level",
            "karma_level", "actor"].map((attr) =>
                p[attr] = participant[attr]);

        // attributes to copy from API to participant_stats
        // with Telemetry, these will be calculated in intervals
        ["kills", "deaths", "assists", "minion_kills",
            "jungle_kills", "non_jungle_minion_kills",
            "crystal_mine_captures", "gold_mine_captures",
            "kraken_captures", "turret_captures",
            "gold", "farm"].map((attr) =>
                p_s[attr] = participant[attr]);

        // score calculations
        var impact_score = -0.28779906 + (p_s.kills * 0.22290324) + (p_s.deaths * -0.50438917) + (p_s.assists * 0.34841597);
        p_s.impact_score = (impact_score + 10) / (10 + 10);


        // traits calculations
        if (roster.hero_kills == 0) p_s.kill_participation = 0;
        else p_s.kill_participation = (p_s.kills + p_s.assists) / roster.hero_kills;

        /* example
        p_s.sustain_score = participant.items.reduce((score, item) => {
            // items[], itemGrants{}, itemUse{}, itemSells{} are the API objects
            // old item names to clean names need to be mapped via `item_name_map[oldname]`
            if (["Eve of Harvest", "Serpent Mask"].indexOf(item_name_map[item]) != -1)
                return score + 20;
            return score;
        }, 0);*/

        return [p, p_s];
    }

    // return "captain" "carry" "jungler"
    function classify_role(participant) {
        return hero_role_map[participant.hero_id];
    }
})();
