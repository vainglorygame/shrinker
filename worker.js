#!/usr/bin/node
/* jshint esnext:true */
'use strict';

var amqp = require("amqplib"),
    Seq = require("sequelize");

var RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    DATABASE_URI = process.env.DATABASE_URI || "sqlite:///db.sqlite",
    BATCHSIZE = process.env.PROCESSOR_BATCH || 50,  // matches
    IDLE_TIMEOUT = process.env.PROCESSOR_IDLETIMEOUT || 500;  // ms

(async () => {
    let seq = new Seq(DATABASE_URI),
        model = require("../orm/model")(seq, Seq),
        rabbit = await amqp.connect(RABBITMQ_URI),
        ch = await rabbit.createChannel();

    let queue = [],
        timer = undefined;

    /* recreate for debugging
    await seq.query("SET FOREIGN_KEY_CHECKS=0");
    await seq.sync({force: true});
    */
    await seq.sync();

    await ch.assertQueue("process", {durable: true});
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
                    let pl = JSON.parse(m.content);
                    pl.attributes.shard_id = m.properties.headers.shard;  // TODO workaround for empty API field
                    return pl;
                }),
                teams = msgs.filter((m) => m.properties.type == "team").map((m) => {
                    let t = JSON.parse(m.content);
                    t.attributes.shard_id = m.properties.headers.shard;
                    return t;
                });

            await Promise.all(matches.map(async (match) => {
                console.log("processing match", match.id);

                // flatten jsonapi nested response into our db structure-like shape
                match.rosters = match.rosters.map((roster) => {
                    roster.participants = roster.participants.map((participant) => {
                        participant.player = flatten(participant.player);
                        return flatten(participant);
                    });
                    return flatten(roster);
                });
                match.assets = match.assets.map((asset) => flatten(asset));
                match = flatten(match);

                // upsert match
                await model.Match.upsert(match, {
                    include: [ model.Roster, model.Asset ]
                });

                // upsert children
                // before, add foreign keys and other missing information (shardId)
                await Promise.all(match.rosters.map(async (roster) => {
                    roster.match_api_id = match.api_id;
                    roster.shard_id = match.shard_id;

                    await model.Roster.upsert(roster, {
                        include: [ model.Participant/*, model.Team */]
                    });

                    await Promise.all(roster.participants.map(async (participant) => {
                        participant.shard_id = roster.shard_id;
                        participant.roster_api_id = roster.api_id;
                        participant.player_api_id = participant.player.api_id;
                        await model.Participant.upsert(participant, {
                            include: [ model.Player ]
                        });
                    }));

                    if (roster.team != null) {
                        roster.team.shard_id = roster.shard_id;
                        roster.team.roster_api_id = roster.api_id;
                        /*await model.Team.upsert(roster.team);*/
                    }
                }));

                await Promise.all(match.assets.map(async (asset) => {
                    asset.match_api_id = match.api_id;
                    asset.shard_id = match.shard_id;
                    await model.Asset.upsert(asset);
                }));
            }));

            // teams and players are upserted seperately
            // because they are duplicated among a page of matches
            // as provided by apigrabber
            console.log("processing", players.length, "players", teams.length, "teams");
            await Promise.all(players.map(async (p) => await model.Player.upsert(flatten(p)) ));
            //await Promise.all(teams.map(async (t) => await model.Team.upsert(flatten(t)) ));

            // COMMIT
            await transaction.commit();
            console.log("acking batch");
            await ch.ack(msgs.pop(), true);  // ack all messages until the last
            // request child jobs, notify player
            await Promise.all(matches.map(async (m) =>
                await Promise.all(m.rosters.map(async (r) =>
                    await Promise.all(r.participants.map(async (p) =>
                        await ch.publish("amq.topic", p.player.name, new Buffer("process_commit"))
                    ))
                ))
            ));
        } catch (err) {  // TODO catch only SQL error, also catch errors in the promises
            console.error(err);
            await ch.nack(msgs.pop(), true, true);  // nack all messages until the last and requeue
            // TODO don't requeue broken records
        }
    }
})();
