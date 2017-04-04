#!/usr/bin/node
/* jshint esnext:true */
'use strict';

var amqp = require("amqplib"),
    Seq = require("sequelize"),
    sleep = require("sleep-promise");

var RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    DATABASE_URI = process.env.DATABASE_URI || "sqlite:///db.sqlite",
    BATCHSIZE = process.env.PROCESSOR_BATCH || 50 * (1 + 5),  // matches + players + teams
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
        let player_ext_records = [],
            participant_ext_records = [],
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
                let player_api_id = player.api_id,
                    player_ext = {};

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

                // calculate "extended" player_ext fields like wins per patch
                player_ext.player_api_id = player_api_id;
                //player_ext.series = ""

                // TODO parallelize

                // TODO maybe this can be done in fewer/combined/subqueries
                let count_matches_where = async (where) => {
                    let record = await model.Participant.findOne({
                        where: where,
                        attributes: [[seq.fn("COUNT", "$roster.match$"), "count"]],
                        include: [ {
                            model: model.Roster,
                            attributes: [],
                            include: [ {
                                model: model.Match,
                                attributes: []
                            } ]
                        } ]
                    })
                    if (record == null) return 0;
                    return record.get("count");
                };

                // TODO run in parallel
                player_ext.played = await model.Participant.count({
                    where: {
                        player_api_id: player_api_id
                    }
                });
                player_ext.wins = await model.Participant.count({
                    where: {
                        player_api_id: player_api_id,
                        winner: true
                    }
                });

                player_ext.played_casual = await count_matches_where({
                    player_api_id: player_api_id,
                    "$roster.match.game_mode$": "casual"
                });
                player_ext.played_ranked = await count_matches_where({
                    player_api_id: player_api_id,
                    "$roster.match.game_mode$": "ranked"
                });
                player_ext.wins_casual = await count_matches_where({
                    player_api_id: player_api_id,
                    winner: true,
                    "$roster.match.game_mode$": "casual"
                });
                player_ext.wins_ranked = await count_matches_where({
                    player_api_id: player_api_id,
                    winner: true,
                    "$roster.match.game_mode$": "ranked"
                });

                player_ext_records.push(player_ext);
            })),
            // calculate participant fields
            Promise.all(participants.map(async (api_participant) => {
                let participant = await model.Participant.findOne({
                    where: {
                        api_id: api_participant.api_id
                    },
                    attributes: ["api_id", "kills", "assists", "deaths", seq.col("roster.hero_kills")],
                    include: [
                        model.Roster
                    ]
                }),
                    participant_ext = {};

                participant_ext.participant_api_id = participant.api_id;
                participant_ext.series = ""  // TODO rm

                if (participant.roster.hero_kills == 0)
                    participant_ext.kills_participation = 0;
                else
                    participant_ext.kills_participation = (participant.kills + participant.assists) / participant.roster.hero_kills;

                if (participant.deaths == 0)
                    participant_ext.kda = 0;
                else
                    participant_ext.kda = (participant.kills + participant.assists) / participant.deaths;

                participant_ext_records.push(participant_ext);
            }))
        ]);

        // load records into db
        try {
            console.log("inserting batch into db");
            await seq.transaction({ autocommit: false }, (transaction) => {
                return Promise.all([
                    model.ParticipantExt.bulkCreate(participant_ext_records, {
                        updateOnDuplicate: [],  // all
                        transaction: transaction
                    }),
                    model.PlayerExt.bulkCreate(player_ext_records, {
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
            await ch.nack(msgs.pop(), true, true);  // nack all messages until the last and requeue
            return;  // give up
        }
        console.log("acking batch");
        await ch.ack(msgs.pop(), true);  // ack all messages until the last

        // notify analyzer
        Promise.all(participant_ext_records.map(async (p) =>
            await ch.sendToQueue("analyze", new Buffer(p.participant_api_id), {
                persistent: true,
                type: "participant"
            })
        ));

        // notify web
        await Promise.all([
            Promise.all(players.map(async (p) => await ch.publish("amq.topic", "player." + p.name,
                new Buffer("stats_update")) )),
            Promise.all(participant_ext_records.map(async (p) => await ch.publish("amq.topic",
                "participant." + p.participant_api_id, new Buffer("stats_update")) ))
        ]);
    }
})();
