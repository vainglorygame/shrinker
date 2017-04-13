#!/usr/bin/node
/* jshint esnext:true */
'use strict';

var amqp = require("amqplib"),
    Seq = require("sequelize"),
    item_name_map = require("../orm/items"),
    hero_name_map = require("../orm/heroes"),
    sleep = require("sleep-promise");

var RABBITMQ_URI = process.env.RABBITMQ_URI,
    DATABASE_URI = process.env.DATABASE_URI,
    // matches + players (2 pages for 100 players)
    BATCHSIZE = parseInt(process.env.BATCHSIZE) || 4 * 50 * (1 + 5),
    IDLE_TIMEOUT = parseFloat(process.env.IDLE_TIMEOUT) || 1000,  // ms
    PREMIUM_FEATURES = process.env.PREMIUM_FEATURES || false;  // calculate on demand for non-premium users

console.log("features for premium users activated", PREMIUM_FEATURES);

let camelCaseRegExp = new RegExp(/([a-z])([A-Z]+)/g);
function camelToSnake(text) {
    return text.replace(camelCaseRegExp, function(m, $1, $2) {
        return $1 + "_" + $2.toLowerCase();
    });
}

function snakeCaseKeys(obj) {
    Object.keys(obj).forEach((key) => {
        let new_key = camelToSnake(key);
        if (new_key == key) return;
        obj[new_key] = obj[key];
        delete obj[key];
    });
    return obj;
}

(async () => {
    let seq, model, rabbit, ch;

    while (true) {
        try {
            seq = new Seq(DATABASE_URI, { logging: () => {} });
            rabbit = await amqp.connect(RABBITMQ_URI, { heartbeat: 30 });
            ch = await rabbit.createChannel();
            await ch.assertQueue("process", {durable: true});
            await ch.assertQueue("crunch", {durable: true});
            await ch.assertQueue("analyze", {durable: true});
            break;
        } catch (err) {
            console.error(err);
            await sleep(5000);
        }
    }

    model = require("../orm/model")(seq, Seq);

    let queue = [],
        timer = undefined;

    let item_db_map = new Map(),      // "Halcyon Potion" to id
        hero_db_map = new Map(),      // "*SAW*" to id
        series_db_map = new Map(),    // date to series id
        game_mode_db_map = new Map(), // "ranked" to id
        role_db_map = new Map(),      // "captain" to id
        hero_role_map = new Map();    // SAW.id to "carry"

    /* recreate for debugging
    await seq.query("SET FOREIGN_KEY_CHECKS=0");
    await seq.sync({force: true});
    */
    // TODO instead of object, use Map
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

    // TODO expire this cache after some time
    let premium_users = (await model.Gamer.findAll()).map((gamer) =>
        gamer.api_id);

    // as long as the queue is filled, msg are not ACKed
    // server sends as long as there are less than `prefetch` unACKed
    await ch.prefetch(BATCHSIZE);

    ch.consume("process", (msg) => {
        queue.push(msg);

        // fill queue until batchsize or idle
        if (timer != undefined) clearTimeout(timer);
        timer = setTimeout(process, IDLE_TIMEOUT);
        if (queue.length == BATCHSIZE)
            process();
    }, { noAck: false });

    async function process() {
        console.log("processing batch", queue.length);

        // clean up to allow processor to accept while we wait for db
        clearTimeout(timer);
        timer = undefined;
        let msgs = queue;
        queue = [];

        // helper to convert API response into flat JSON
        // db structure is (almost) 1:1 the API structure
        // so we can insert the flat API response as-is
        function flatten(obj) {
            let attrs = obj.attributes || {},
                stats = attrs.stats || {},
                o = Object.assign({}, obj, attrs, stats);
            o.api_id = o.id;  // rename
            delete o.id;
            delete o.type;
            delete o.attributes;
            delete o.stats;
            delete o.relationships;
            return snakeCaseKeys(o);
        }

        // to sort out duplicates
        // TODO refactor this
        // TODO use `Set` where appropriate
        let processed_players = [];

        // we aggregate record objects to do a bulk insert
        let match_records = [],
            roster_records = [],
            participant_records = [],
            participant_stats_records = [],
            player_records = [],
            asset_records = [],
            participant_item_records = [],
            player_msgs = msgs.filter((m) => m.properties.type == "player"),
            match_msgs = msgs.filter((m) => m.properties.type == "match"),
            player_objects = [].concat(...player_msgs.map((msg) =>
                JSON.parse(msg.content))),
            match_objects = match_msgs.map((msg) => JSON.parse(msg.content)),
            notify_players = [];  // players that have new matches

        // reject duplicates
        await Promise.all(match_objects.map(async (match, idx) => {
            if (await model.Match.count({
                where: { api_id: match.id }
            }) > 0) delete match_objects[idx];
        }));
        match_objects = match_objects.filter((match, idx, self) =>
            self.indexOf(match) === idx);
        player_objects = player_objects.filter((player, idx, self) =>
            self.indexOf(player) === idx);

        // populate `_records`
        // data from `/player`
        player_objects.forEach((p) => {
            let player = flatten(p);
            // player objects that arrive here came from a search
            // with search, updater can't update last_update
            player.last_update = seq.fn("NOW");
            console.log("processing player", player.name);
            processed_players.push(player.api_id);
            player_records.push(player);
        });

        // reject invalid matches (handling API bugs)
        match_objects.forEach((match, idx) => {
            if (match.rosters[0].id == "null")
                delete match_objects[idx];
        });

        // data from `/matches`
        match_objects.forEach((match) => {
            console.log("processing match", match.id);

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
                    let itms = [],
                        item_use = (arr, action) =>
                            arr.map((item) => { return {
                                participant_api_id: participant.id,
                                item_id: item_db_map[item_name_map[item]],
                                action: action
                            } }),
                        item_arr_from_obj = (obj) =>
                            [].concat(...  // 3 flatten
                                Object.entries(obj).map(  // 1 map over (key, value)
                                    (tuple) => Array(tuple[1]).fill(tuple[0])))  // 2 create Array [key] * value

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
                    let p_pstats = calculate_participant_stats(match, r, p);
                    // participant gets split into participant and p_stats
                    participant_records.push(p_pstats[0]);
                    participant_stats_records.push(p_pstats[1]);
                    // deduplicate player
                    // in a batch, it is very likely that players are duplicated
                    // so this improves performance a bit
                    if (processed_players.indexOf(p.player.api_id)) {
                        processed_players.push(p.player.api_id);
                        player_records.push(p.player);
                        notify_players.push(p.player.api_id);
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

        // now access db
        try {
            // upsert whole batch in parallel
            await seq.query("SET unique_checks=0");
            console.log("inserting batch into db");
            await seq.transaction({ autocommit: false }, (transaction) => {
                return Promise.all([
                    Promise.all([
                        model.Match.bulkCreate(match_records, {
                            include: [ model.Roster, model.Asset ],
                            ignoreDuplicate: true,
                            transaction: transaction
                        }),
                        model.Roster.bulkCreate(roster_records, {
                            include: [ model.Roster ],
                            ignoreDuplicate: true,
                            transaction: transaction
                        }),
                        model.Participant.bulkCreate(participant_records, {
                            include: [ model.Player ],
                            ignoreDuplicate: true,
                            transaction: transaction
                        }),
                        model.ParticipantStats.bulkCreate(participant_stats_records, {
                            include: [ model.Participant ],
                            ignoreDuplicate: true,
                            transaction: transaction
                        }),
                        model.Player.bulkCreate(player_records, {
                            updateOnDuplicate: [
                                "shard_id", "api_id", "name"
                            ],
                            transaction: transaction
                        }),
                        model.ItemParticipant.bulkCreate(participant_item_records, {
                            include: [ model.Participant ],
                            ignoreDuplicate: true,
                            transaction: transaction
                        }),
                        model.Asset.bulkCreate(asset_records, {
                            ignoreDuplicate: true,
                            transaction: transaction
                        })
                    ]),

                    // update last_match_created_date and skill tier for players
                    // that were explicitely pushed into processor
                    Promise.all(player_objects.map(async (player) => {
                        // set last_match_created_date
                        console.log("updating player", player.attributes.name);
                        let record = await model.Participant.findOne({
                            transaction: transaction,
                            where: { player_api_id: player.id },
                            attributes: [
                                [seq.col("created_at"), "last_match_created_date"],
                                "skill_tier"
                            ],
                            order: [ [seq.col("created_at"), "DESC"] ]
                        });
                        if (record != null) {
                            await model.Player.update({
                                last_match_created_date: record.get("last_match_created_date"),
                                skill_tier: record.get("skill_tier")
                            }, {
                                where: { api_id: player.id },
                                fields: [ "last_match_created_date", "skill_tier" ],
                                transaction: transaction
                            });
                        }
                    }))
                ]);
            });
            await seq.query("SET unique_checks=1");

            console.log("acking batch");
            await Promise.all(msgs.map((m) => ch.ack(m)) );
        } catch (err) {
            // this should only happen for Deadlocks in prod
            // it *must not* fail due to broken schema or missing dependency
            // TODO: eliminate such cases earlier in the chain
            // and immediately NACK those broken matches, requeueing only the rest
            console.error(err);
            await Promise.all(msgs.map((m) => ch.nack(m, true)) );  // requeue
            return;  // give up
        }

        // notify web
        // …about new matches
        await Promise.all(notify_players.map(async (api_id) =>
            await ch.publish("amq.topic", "player." + api_id, new Buffer("matches_update")) ));
        // …about updated lifetime stats
        await Promise.all(player_objects.map(async (api_player) =>
            await ch.publish("amq.topic", "player." + api_player.id, new Buffer("stats_update")) ));
        // …global about new matches
        if (match_records.length > 0)
            await ch.publish("amq.topic", "global", new Buffer("matches_update"));

        // notify analyzer & cruncher
        Promise.all(participant_stats_records.map(async (p) =>
            await ch.sendToQueue("analyze", new Buffer(p.participant_api_id), {
                persistent: true,
                type: "participant"
            })
        ));
        Promise.all(notify_players.map(async (api_id) => {
            if (PREMIUM_FEATURES == false || premium_users.indexOf(api_id) != -1) {
                await ch.sendToQueue("crunch", new Buffer(api_id), {
                    persistent: true,
                    type: "player"
                })
            }
        }));
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
        if (p_s.created_at < new Date(2017, 3, 28))
            p.series_id = series_db_map["Patch 2.2"];
        else
            p.series_id = series_db_map["Patch 2.3"];
        p.game_mode_id = game_mode_db_map[match.game_mode];
        p.role_id = role_db_map[classify_role(p)];

        // attributes to copy from API to participant
        // these don't change over the duration of the match
        // (or aren't in Telemetry)
        ["api_id", "shard_id", "player_api_id", "roster_api_id",
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

        // traits calculations
        if (roster.hero_kills == 0) p_s.kill_participation = 0;
        else p_s.kill_participation = (p_s.kills + p_s.assists) / roster.hero_kills;

        p_s.sustain_score = participant.items.reduce((score, item) => {
            // items[], itemGrants{}, itemUse{}, itemSells{} are the API objects
            // old item names to clean names need to be mapped via `item_name_map[oldname]`
            if (["Eve of Harvest", "Serpent Mask"].indexOf(item_name_map[item]) != -1)
                return score + 20;
            return score;
        }, 0);

        return [p, p_s];
    }

    // return "captain" "carry" "jungler"
    function classify_role(participant) {
        return hero_role_map[participant.hero_id];
    }
})();
