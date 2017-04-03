#!/usr/bin/node
/* jshint esnext:true */
'use strict';

var amqp = require("amqplib"),
    Seq = require("sequelize"),
    snakeCaseKeys = require("snakecase-keys"),
    item_name_map = require("../orm/items");

var RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    DATABASE_URI = process.env.DATABASE_URI || "sqlite:///db.sqlite",
    BATCHSIZE = process.env.PROCESSOR_BATCH || 50 * (6 + 5),  // objects
    IDLE_TIMEOUT = process.env.PROCESSOR_IDLETIMEOUT || 500;  // ms

(async () => {
    let seq = new Seq(DATABASE_URI),
        model = require("../orm/model")(seq, Seq),
        rabbit = await amqp.connect(RABBITMQ_URI),
        ch = await rabbit.createChannel();

    let queue = [],
        timer = undefined;

    let item_db_map = {};  // "Halcyon Potion" to id

    /* recreate for debugging
    await seq.query("SET FOREIGN_KEY_CHECKS=0");
    await seq.sync({force: true});
    */
    await seq.sync();
    // TODO instead of object, use Map
    await model.Item.findAll()
        .map((item) => item_db_map[item.name] = item.id);

    await ch.assertQueue("process", {durable: true});
    await ch.assertQueue("compile", {durable: true});
    // as long as the queue is filled, msg are not ACKed
    // server sends as long as there are less than `prefetch` unACKed
    await ch.prefetch(BATCHSIZE);

    ch.consume("process", async (msg) => {
        queue.push(msg);

        // fill queue until batchsize or idle
        if (timer === undefined)
            timer = setTimeout(process, IDLE_TIMEOUT)
        if (queue.length == BATCHSIZE)
            await process();
    }, { noAck: false });

    async function process() {
        console.log("processing batch", queue.length);

        // clean up to allow processor to accept while we wait for db
        let msgs = queue.slice();
        queue = [];
        clearTimeout(timer);
        timer = undefined;

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
        let is_in = (arr, obj) => arr.map((o) => o.api_id).indexOf(obj.api_id) > -1;


        // we aggregate record objects to do a bulk insert
        let match_records = [],
            roster_records = [],
            participant_records = [],
            player_records = [],
            asset_records = [],
            participant_item_records = [];

        // populate `_records`
        // data from `/player`
        msgs.filter((m) => m.properties.type == "player").map((msg) => {
            let players = JSON.parse(msg.content);
            players.map(async (p) => {
                player = flatten(p);
                if (!is_in(player_records, p.player))
                    player_records.push(p.player);
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

                roster.participants = roster.participants.map((participant) => {
                    participant.attributes.shardId = roster.attributes.shardId;
                    participant.rosterApiId = roster.id;
                    participant.createdAt = roster.createdAt;
                    participant.playerApiId = participant.player.id;

                    // API bug fixes
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
                    participant.attributes.stats.items = itms;

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
                        participant_records.push(p);
                        // deduplicate player
                        // in a batch, it is very likely that players are duplicated
                        // so this improves performance a bit
                        if (!is_in(player_records, p.player)) player_records.push(p.player);
                        p.items.map((i) => {
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
            await seq.transaction({ autocommit: false }, (transaction) => {
                return Promise.all([
                    model.Match.bulkCreate(match_records, {
                        include: [ model.Roster, model.Asset ],
                        updateOnDuplicate: [],  // all
                        transaction: transaction
                    }),
                    model.Roster.bulkCreate(roster_records, {
                        include: [ model.Roster ],
                        updateOnDuplicate: [],  // all
                        transaction: transaction
                    }),
                    model.Participant.bulkCreate(participant_records, {
                        include: [ model.Player ],
                        updateOnDuplicate: [],  // all
                        transaction: transaction
                    }),
                    model.Player.bulkCreate(player_records, {
                        updateOnDuplicate: [],  // all
                        transaction: transaction
                    }),
                    model.ParticipantItemUse.bulkCreate(participant_item_records, {
                        include: [ model.Participant ],
                        updateOnDuplicate: [],  // all
                        transaction: transaction
                    }),
                    model.Asset.bulkCreate(asset_records, {
                        updateOnDuplicate: [],  // all
                        transaction: transaction
                    })
                ]);
            });
        } catch (err) {
            // this should only happen for Deadlocks in prod
            // it *must not* fail due to broken schema or missing dependency
            // TODO: eliminate such cases earlier in the chain
            // and immediately NACK those broken matches, requeueing only the rest
            console.error(err);
            await ch.nack(msgs.pop(), true, true);  // nack all messages until the last and requeue
            return;  // give up
        }

        console.log("acking batch");
        await ch.ack(msgs.pop(), true);  // ack all messages until the last

        // notify compiler
        await Promise.all([
            Promise.all(participant_records.map(async (p) =>
                await ch.sendToQueue("compile", new Buffer(JSON.stringify(p)), {
                    persistent: true,
                    type: "participant"
                })
            )),
            Promise.all(player_records.map(async (p) =>
                await ch.sendToQueue("compile", new Buffer(JSON.stringify(p)), {
                    persistent: true,
                    type: "player"
                })
            ))
        ]);

        // notify web
        await Promise.all(player_records.map(async (p) =>
            await ch.publish("amq.topic", p.name, new Buffer("process_commit")) ));
        if (match_records.length > 0)
            await ch.publish("amq.topic", "global", new Buffer("matches_update"));
    }
})();
