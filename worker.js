#!/usr/bin/node
/* jshint esnext:true */
'use strict';

var amqp = require("amqplib"),
    Seq = require("sequelize"),
    snakeCaseKeys = require("snakecase-keys"),
    item_name_map = require("../orm/items"),
    hero_name_map = require("../orm/heroes"),
    sleep = require("sleep-promise");

var RABBITMQ_URI = process.env.RABBITMQ_URI,
    DATABASE_URI = process.env.DATABASE_URI,
    BATCHSIZE = parseInt(process.env.PROCESSOR_BATCH) || 50 * (1 + 5),  // matches + players
    IDLE_TIMEOUT = parseFloat(process.env.PROCESSOR_IDLETIMEOUT) || 500;  // ms

(async () => {
    let seq, model, rabbit, ch;

    while (true) {
        try {
            seq = new Seq(DATABASE_URI, { logging: () => {} });
            rabbit = await amqp.connect(RABBITMQ_URI);
            ch = await rabbit.createChannel();
            await ch.assertQueue("process", {durable: true});
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

    let item_db_map = {},      // "Halcyon Potion" to id
        hero_db_map = {},      // "*SAW*" to id
        series_db_map = {},    // date to series id
        game_mode_db_map = {}, // "ranked" to id
        role_db_map = {};      // "captain" to id

    /* recreate for debugging
    await seq.query("SET FOREIGN_KEY_CHECKS=0");
    await seq.sync({force: true});
    */
    // TODO instead of object, use Map
    await Promise.all([
        model.Item.findAll()
            .map((item) => item_db_map[item.name] = item.id),
        model.Hero.findAll()
            .map((hero) => hero_db_map[hero.name] = hero.id),
        model.Series.findAll()
            .map((series) => series_db_map[series.name] = series.id),
        model.GameMode.findAll()
            .map((mode) => game_mode_db_map[mode.name] = mode.id),
        model.Role.findAll()
            .map((role) => role_db_map[role.name] = role.id)
    ]);

    // as long as the queue is filled, msg are not ACKed
    // server sends as long as there are less than `prefetch` unACKed
    await ch.prefetch(BATCHSIZE);

    ch.consume("process", async (msg) => {
        queue.push(msg);

        // fill queue until batchsize or idle
        if (timer === undefined)
            timer = setTimeout(process, IDLE_TIMEOUT);
        if (queue.length == BATCHSIZE)
            await process();
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
            return o;
        }

        // helper: true if object is not in record arr
        // also, sort out invalid objects (undefined or null)
        let is_in = (arr, obj) => obj == undefined ||
            arr.map((o) => o.api_id).indexOf(obj.api_id) > -1;


        // we aggregate record objects to do a bulk insert
        let match_records = [],
            roster_records = [],
            participant_records = [],
            participant_stats_records = [],
            player_records = [],
            asset_records = [],
            participant_item_records = [];

        // populate `_records`
        // data from `/player`
        msgs.filter((m) => m.properties.type == "player").map((msg) => {
            let players = JSON.parse(msg.content);
            players.map((p) => {
                let player = flatten(p);
                console.log("processing player", player.name);
                if (!is_in(player_records, player))
                    player_records.push(player);
            });
        });

        // data from `/matches`
        msgs.filter((m) => m.properties.type == "match").map((msg) => {
            let match = JSON.parse(msg.content);
            console.log("processing match", match.id);

            // flatten jsonapi nested response into our db structure-like shape
            // also, push missing fields and snakecasify
            match.rosters = match.rosters.map((roster) => {
                roster.matchApiId = match.id;
                roster.attributes.shardId = match.attributes.shardId;
                roster.createdAt = match.createdAt;
                // TODO API workaround: roster does not have `winner`
                roster.attributes.stats.winner = roster.participants[0].stats.winner;

                roster.participants = roster.participants.map((participant) => {
                    participant.attributes.shardId = roster.attributes.shardId;
                    participant.rosterApiId = roster.id;
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
            match = snakeCaseKeys(flatten(match));

            // after conversion, create the array of records
            // there is a low chance of a match being duplicated in a batch,
            // skip it and its children
            if (!is_in(match_records, match)) {
                match_records.push(match);
                match.rosters.map((r) => {
                    roster_records.push(r);
                    r.participants.map((p) => {
                        let p_pstats = calculate_participant_stats(match, r, p);
                        // participant gets split into participant and p_stats
                        participant_records.push(p_pstats[0]);
                        participant_stats_records.push(p_pstats[1]);
                        // deduplicate player
                        // in a batch, it is very likely that players are duplicated
                        // so this improves performance a bit
                        if (!is_in(player_records, p.player)) player_records.push(p.player);
                        p.participant_items.map((i) => {
                            participant_item_records.push(i);
                        });
                    });
                });
                match.assets.map((a) => {
                    asset_records.push(a);
                });
            }
        });

        // now access db
        try {
            console.log("inserting batch into db");
            // upsert whole batch in parallel
            await seq.transaction({ autocommit: false }, async (transaction) => {
                await Promise.all([
                    model.Match.bulkCreate(match_records, {
                        include: [ model.Roster, model.Asset ],
                        updateOnDuplicate: [],  // all
                        transaction: transaction
                    }),
                    model.Roster.bulkCreate(roster_records, {
                        include: [ model.Roster ],
                        updateOnDuplicate: [],
                        transaction: transaction
                    }),
                    model.Participant.bulkCreate(participant_records, {
                        include: [ model.Player ],
                        updateOnDuplicate: [],
                        transaction: transaction
                    }),
                    model.ParticipantStats.bulkCreate(participant_stats_records, {
                        include: [ model.Participant ],
                        updateOnDuplicate: [],
                        transaction: transaction
                    }),
                    model.Player.bulkCreate(player_records, {
                        updateOnDuplicate: [],
                        transaction: transaction
                    }),
                    model.ItemParticipant.bulkCreate(participant_item_records, {
                        include: [ model.Participant ],
                        updateOnDuplicate: [],
                        transaction: transaction
                    }),
                    model.Asset.bulkCreate(asset_records, {
                        updateOnDuplicate: [],
                        transaction: transaction
                    })
                ]);
            });
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
    /*
        // collect information and populate _record arrays
        await Promise.all([
            // collect player information
            Promise.all(players.map(async (player) => {
                let player_api_id = player.api_id;

                // set last_match_created_date
                let record = await model.Participant.findOne({
                    where: {
                        player_api_id: player_api_id
                    },
                    attributes: [ [seq.col("roster.match.created_at"), "last_match_created_date"] ],
                    include: [ {
                        model: model.Roster,
                        attributes: [],
                        include: [ {
                            model: model.Match,
                            attributes: []
                        } ]
                    } ],
                    order: [
                        [seq.col("last_match_created_date"), "DESC"]
                    ]
                });
                if (record != null)
                    // do later in the transaction
                    player_updates.push([
                        { last_match_created_date: record.get("last_match_created_date") },
                        { where: { api_id: player_api_id } }
                    ]);
            })),
        ]);

        // load records into db
        try {
            console.log("inserting batch into db");
            await seq.transaction({ autocommit: false }, async (transaction) => {
                await Promise.all([
                    player_updates.map(async (pu) =>
                        await model.Player.update(pu[0], pu[1]))
                ]);
            });
    */

        // notify web
        await Promise.all(player_records.map(async (p) =>
            await ch.publish("amq.topic", "player." + p.name, new Buffer("matches_update")) ));
        if (match_records.length > 0)
            await ch.publish("amq.topic", "global", new Buffer("matches_update"));

        // notify analyzer
        Promise.all(participant_stats_records.map(async (p) =>
            await ch.sendToQueue("analyze", new Buffer(p.participant_api_id), {
                persistent: true,
                type: "participant"
            })
        ));
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
        p_s.created_at = new Date();  // TODO set to match.created_at+match.duration
        p.participant_items = participant.participant_items;
        // mappings
        // hero names additionally need to be mapped old to new names
        // (Sayoc = Taka)
        p.hero_id = hero_db_map[hero_name_map[participant.actor]];
        // TODO don't hardcode this, waiting for `match.patch_version` to be available
        p.series_id = series_db_map["Patch 2.3"];
        p.game_mode_id = game_mode_db_map[match.game_mode];
        p.role_id = series_db_map["all"];  // TODO identify roles

        // attributes to copy from API to participant
        // these don't change over the duration of the match
        // (or aren't in Telemetry)
        ["api_id", "shard_id", "player_api_id", "roster_api_id",
            "winner", "went_afk", "first_afk_time",
            "skin_key", "skill_tier", "level",
            "karma_level", "actor",
            "farm"].map((attr) =>
                p[attr] = participant[attr]);

        // attributes to copy from API to participant_stats
        // with Telemetry, these will be calculated in intervals
        ["kills", "deaths", "assists", "minion_kills",
            "jungle_kills", "non_jungle_minion_kills",
            "crystal_mine_captures", "gold_mine_captures",
            "kraken_captures", "turret_captures",
            "gold"].map((attr) =>
                p_s[attr] = participant[attr]);

        // traits calculations
        if (roster.hero_kills == 0) p_s.kill_participation = 0;
        else p_s.kill_participation = p_s.kills / roster.hero_kills;

        p_s.sustain_score = participant.items.reduce((score, item) => {
            // items[], itemGrants{}, itemUse{}, itemSells{} are the API objects
            // old item names to clean names need to be mapped via `item_name_map[oldname]`
            if (["Eve of Harvest", "Serpent Mask"].indexOf(item_name_map[item]) != -1)
                return score + 20;
            return score;
        }, 0);

        return [p, p_s];
    }
})();
