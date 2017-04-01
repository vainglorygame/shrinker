#!/usr/bin/node
/* jshint esnext:true */
'use strict';

var amqp = require("amqplib"),
    Seq = require("sequelize");

var RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    DATABASE_URI = process.env.DATABASE_URI || "sqlite:///db.sqlite",
    BATCHSIZE = process.env.PROCESSOR_BATCH || 50 * (1 + 5),  // matches + players + teams
    IDLE_TIMEOUT = process.env.PROCESSOR_IDLETIMEOUT || 500;  // ms

(async () => {
    let seq = new Seq(DATABASE_URI),
        model = require("../orm/model")(seq, Seq),
        rabbit = await amqp.connect(RABBITMQ_URI),
        ch = await rabbit.createChannel();

    let queue = [],
        timer = undefined;

    await seq.sync();

    await ch.assertQueue("compile", {durable: true});
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

        // BEGIN
        let transaction = await seq.transaction({ autocommit: false });

        // UPSERT
        try {
            // processor sends to queue with a custom "type" so compiler can filter
            // m.content: player.api_id
            let players      = msgs.filter((m) => m.properties.type == "player").map((m) => JSON.parse(m.content)),
                participants = msgs.filter((m) => m.properties.type == "participant").map((m) => JSON.parse(m.content));

            await Promise.all(players.map(async (player) => {
                let player_api_id = player.api_id,
                    player_ext = {};

                player_ext.player_api_id = player_api_id;
                //player_ext.series = ""

                // TODO parallelize

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

                // TODO maybe this can be done in fewer/combined/subqueries
                let count_matches_where = async (where) => {
                    return (await model.Participant.findOne({
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
                    })).get("count");
                };
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

                await model.PlayerExt.upsert(player_ext, {
                    include: [ model.Participant ],
                    transaction: transaction
                });
            }));
            await Promise.all(participants.map(async (api_participant) => {
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

                await model.ParticipantExt.upsert(participant_ext, {
                    include: [ model.Participant ],
                    transaction: transaction
                });
            }));

            // COMMIT
            await transaction.commit();
            console.log("acking batch");
            await ch.ack(msgs.pop(), true);  // ack all messages until the last

            // notify web
            await Promise.all(players.map(async (p) => await ch.publish("amq.topic", p.name, new Buffer("compile_commit")) ));
        } catch (err) {  // TODO catch only SQL error, also catch errors in the promises
            console.error(err);
            await transaction.rollback();
            await ch.nack(msgs.pop(), true, true);  // nack all messages until the last and requeue
            // TODO don't requeue broken records
        }
    }
})();
