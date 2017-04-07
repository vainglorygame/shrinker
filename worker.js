#!/usr/bin/node
/* jshint esnext:true */
'use strict';

var amqp = require("amqplib"),
    Seq = require("sequelize"),
    sleep = require("sleep-promise");

var RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    DATABASE_URI = process.env.DATABASE_URI || "sqlite:///db.sqlite",
    BATCHSIZE = process.env.PROCESSOR_BATCH || 50 * (1 + 5),  // matches + players
    IDLE_TIMEOUT = process.env.PROCESSOR_IDLETIMEOUT || 500;  // ms

(async () => {
    let seq, model, rabbit, ch;

    while (true) {
        try {
            seq = new Seq(DATABASE_URI, { logging: () => {} }),
            rabbit = await amqp.connect(RABBITMQ_URI),
            ch = await rabbit.createChannel();
            await ch.assertQueue("compile", {durable: true});
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

    await seq.sync();

    // as long as the queue is filled, msg are not ACKed
    // server sends as long as there are less than `prefetch` unACKed
    await ch.prefetch(BATCHSIZE);

    ch.consume("compile", async (msg) => {
        queue.push(msg);

        // fill queue until batchsize or idle
        if (timer === undefined)
            timer = setTimeout(process, IDLE_TIMEOUT)
        if (queue.length == BATCHSIZE)
            await process();
    }, { noAck: false });

    async function process() {
        console.log("compiling batch", queue.length);

        // clean up to allow processor to accept while we wait for db
        let msgs = queue.slice();
        queue = [];
        clearTimeout(timer);
        timer = undefined;

        // aggregate & bulk insert
        let participant_stats_records = [],
            player_updates = [];  // [[what, where]]

        // processor sends to queue with a custom "type" so compiler can filter
        // m.content: player.api_id
        let players      = msgs.filter((m) => m.properties.type == "player").map(
            (m) => JSON.parse(m.content)).filter((p) => p != undefined),
            participants = msgs.filter((m) => m.properties.type == "participant").map(
            (m) => JSON.parse(m.content)).filter((p) => p != undefined);

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
            // calculate participant fields
            Promise.all(participants.map(async (api_participant) => {
                participant_stats_records.push(calculate_stats(
                    await model.Participant.findOne({
                        where: { api_id: api_participant.api_id },
                        include: [ {
                                model: model.Roster,
                                include: [
                                    model.Match
                                ]
                            }, {
                                model: model.ItemParticipant,
                                as: "items",
                                include: [ model.Item ]
                            }
                        ]
                    })
                ))
            }))
        ]);

        // load records into db
        try {
            console.log("inserting batch into db");
            await seq.transaction({ autocommit: false }, async (transaction) => {
                await Promise.all([
                    model.ParticipantStats.bulkCreate(participant_stats_records, {
                        updateOnDuplicate: [],  // all
                        transaction: transaction
                    }),
                    player_updates.map(async (pu) =>
                        await model.Player.update(pu[0], pu[1]))
                ]);
            });
        } catch (err) {
            // this should only happen for deadlocks or non-data related issues
            console.error(err);
            await Promise.all(msgs.map((m) => ch.nack(m, true)) );  // requeue
            return;  // give up
        }
        console.log("acking batch");
        await Promise.all(msgs.map((m) => ch.ack(m)) );

        // notify analyzer
        Promise.all(participant_stats_records.map(async (p) =>
            await ch.sendToQueue("analyze", new Buffer(p.participant_api_id), {
                persistent: true,
                type: "participant"
            })
        ));

        // notify web
        await Promise.all([
            Promise.all(players.map(async (p) => await ch.publish("amq.topic", "player." + p.name,
                new Buffer("stats_update")) )),
            Promise.all(participant_stats_records.map(async (p) => await ch.publish("amq.topic",
                "participant." + p.participant_api_id, new Buffer("stats_update")) ))
        ]);
    }

    // based on the participant db record from the end of the match,
    // calculate a participant_stats record and return it
    function calculate_stats(participant) {
        if (participant == null) { console.error("got nonexisting participant!"); return; }
        let participant_stats = {};
        participant_stats.participant_api_id = participant.get("api_id");

        participant_stats.kills = participant.get("kills");

        if (participant.roster.get("hero_kills") == 0) participant_stats.kill_participation = 0;
        else participant_stats.kill_participation = participant.get("kills") / participant.roster.get("hero_kills");

        participant_stats.sustain_score = participant.items.reduce((score, item) => {
            if (item.get("action") == "final") {
                if (["Eve of Harvest", "Serpent Mask"].indexOf(item.item.get("name")) != -1)
                    return score + 20;
            }
            return score;
        }, 0);

        return participant_stats;
    }
})();
