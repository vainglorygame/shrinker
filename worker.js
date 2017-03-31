#!/usr/bin/node
/* jshint esnext:true */
'use strict';

var amqp = require("amqplib"),
    Seq = require("sequelize");

var RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    DATABASE_URI = process.env.DATABASE_URI || "sqlite:///db.sqlite",
    BATCHSIZE = process.env.PROCESSOR_BATCH || 50 * (1 + 2 + 3*2 + 3*2),
    IDLE_TIMEOUT = process.env.PROCESSOR_IDLETIMEOUT || 500;  // ms

(async () => {
    let seq = new Seq(DATABASE_URI),
        model = require("./model")(seq, Seq),
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
            process();
    }, { noAck: false });

    async function process() {
        console.log("processing batch");

        // clean up to allow processor to accept while we wait for db
        let matchmsgs = queue.slice();
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
            let matches = matchmsgs.map((msg) => JSON.parse(msg.content));
            await matches.forEach(async (match) => {
                // flatten jsonapi nested response into our db structure-like shape
                match.rosters = match.rosters.map((roster) => {
                    roster.participants = roster.participants.map((participant) => {
                        participant.player = flatten(participant.player);
                        return flatten(participant);
                    });
                    return flatten(roster);
                });
                match = flatten(match);

                // upsert match
                await model.Match.upsert(match, {
                    include: [ model.Roster, model.Asset ]
                });

                // upsert children
                // before, add foreign keys and other missing information (shardId)
                await match.rosters.forEach(async (roster) => {
                    roster.match_api_id = match.api_id;
                    roster.shard_id = match.shard_id;

                    await model.Roster.upsert(roster, {
                        include: [ model.Participant, model.Team ]
                    });

                    await roster.participants.forEach(async (participant) => {
                        participant.player.shard_id = participant.shard_id;
                        await model.Player.upsert(participant.player);

                        participant.shard_id = roster.shard_id;
                        participant.roster_api_id = roster.api_id;
                        participant.player_api_id = participant.player.api_id;
                        await model.Participant.upsert(participant, {
                            include: [ model.Player ]
                        });
                    });

                    if (roster.team != null) {
                        roster.team.shard_id = roster.shard_id;
                        roster.team.roster_api_id = roster.api_id;
                        await model.Team.upsert(roster.team);
                    }
                });

                await match.assets.forEach(async (asset) => {
                    await model.Asset.upsert(asset);
                });
            });

            // COMMIT
            await transaction.commit();
            await ch.ack(matchmsgs.pop(), true);  // ack all messages until the last
            // request child jobs, notify player
            await matches.forEach(async (m) => {
                await m.rosters.forEach(async (r) => {
                    await r.participants.forEach(async (p) => {
                        await ch.publish("amq.topic", p.player.name, new Buffer("process_commit"));
                    });
                });
            });
        } catch (err) {  // TODO catch only SQL error, also catch errors in the forEach
            console.error(err);
            await ch.nack(matchmsgs.pop(), true, true);  // nack all messages until the last and requeue
            // TODO don't requeue broken records
        }
    }
})();
