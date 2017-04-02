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

        // BEGIN
        let transaction = await seq.transaction({ autocommit: false });

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

        // UPSERT
        try {
            // apigrabber sends to queue with a custom "type" so processor can filter
            // and insert players and matches seperately
            let matches = msgs.filter((m) => m.properties.type == "match").map((m) => JSON.parse(m.content)),
                players = msgs.filter((m) => m.properties.type == "player").map((m) => {
                    let pl = flatten(JSON.parse(m.content));
                    pl.shard_id = m.properties.headers.shard;  // TODO workaround for empty API field
                    return pl;
                });

            await Promise.all(matches.map(async (match) => {
                console.log("processing match", match.id);

                // flatten jsonapi nested response into our db structure-like shape
                // also, push missing fields and snakecasify
                match.rosters = match.rosters.map((roster) => {
                    roster.matchApiId = match.id;
                    roster.shardId = match.shardId;
                    roster.createdAt = match.createdAt;

                    roster.participants = roster.participants.map((participant) => {
                        participant.shardId = roster.shardId;
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

                        participant.player = snakeCaseKeys(flatten(participant.player));
                        return snakeCaseKeys(flatten(participant));
                    });
                    return snakeCaseKeys(flatten(roster));
                });
                match.assets = match.assets.map((asset) => {
                    asset.matchApiId = match.id;
                    asset.shardId = match.shardId;
                    return snakeCaseKeys(flatten(asset));
                });
                match = snakeCaseKeys(flatten(match));

                // upsert match
                await model.Match.upsert(match, {
                    include: [ model.Roster, model.Asset ],
                    transaction: transaction
                });

                // upsert children
                // before, add foreign keys and other missing information (shardId)
                await Promise.all(match.rosters.map(async (roster) => {
                    await model.Roster.upsert(roster, {
                        include: [ model.Participant ],
                        transaction: transaction
                    });
                    await Promise.all(roster.participants.map(async (participant) => {
                        await model.Participant.upsert(participant, {
                            include: [ model.Player ],
                            transaction: transaction
                        });
                        await Promise.all(participant.items.map(async (item) =>
                            await model.ParticipantItemUse.upsert(item, {
                                include: [ model.Participant ],
                                transaction: transaction
                            })
                        ));
                    }));
                }));

                await Promise.all(match.assets.map(async (asset) => {
                    await model.Asset.upsert(asset, { transaction: transaction });
                }));
            }));

            // players are upserted seperately
            // because they are duplicated among a page of matches
            // as provided by apigrabber
            console.log("processing", players.length, "players");
            await Promise.all(players.map(async (p) => await model.Player.upsert(snakeCaseKeys(p), { transaction: transaction }) ));

            // COMMIT
            await transaction.commit();
            console.log("acking batch");
            await ch.ack(msgs.pop(), true);  // ack all messages until the last

            // notify web
            await Promise.all(players.map(async (p) => await ch.publish("amq.topic", p.name, new Buffer("process_commit")) ));
            // notify compiler
            await Promise.all(matches.map(async (m) => {
                await Promise.all(m.rosters.map(async (r) => {
                    await Promise.all(r.participants.map(async (p) => {
                        await ch.sendToQueue("compile", new Buffer(JSON.stringify(p)), {
                            persistent: true,
                            type: "participant"
                        });
                    }));
                }));
            }));
            await Promise.all(players.map(async (p) =>
                await ch.sendToQueue("compile", new Buffer(JSON.stringify(p)), {
                    persistent: true,
                    type: "player"
                })
            ));
        } catch (err) {  // TODO catch only SQL error, also catch errors in the promises
            console.error(err);
            await transaction.rollback();
            await ch.nack(msgs.pop(), true, true);  // nack all messages until the last and requeue
            // TODO don't requeue broken records
        }
    }
})();
