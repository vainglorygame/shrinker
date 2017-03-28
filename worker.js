#!/usr/bin/node
/* jshint esnext:true */
'use strict';

var amqp = require("amqplib"),
    Seq = require("sequelize"),
    Bluebird = require("bluebird"),
    jsonapi = Bluebird.promisifyAll(require("superagent-jsonapify/common"));

var MADGLORY_TOKEN = process.env.MADGLORY_TOKEN,
    RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    DATABASE_URI = process.env.DATABASE_URI || "sqlite:///db.sqlite";
if (MADGLORY_TOKEN == undefined) throw "Need an API token";

(async () => {
    let seq = new Seq(DATABASE_URI),
        model = require("./model")(seq, Seq),
        rabbit = await amqp.connect(RABBITMQ_URI),
        ch = await rabbit.createChannel();

    /* recreate for debugging
    await seq.query("SET FOREIGN_KEY_CHECKS=0");
    await seq.sync({force: true});
    */
    await seq.sync();

    await ch.assertQueue("process", {durable: true});
    await ch.prefetch(1);

    ch.consume("process", async (msg) => {
        let data = await jsonapi.parse(msg.content);

        // TODO commit less often if possible, avoid deadlocks
        let transaction = await seq.transaction({ autocommit: false });

        data.data.forEach((match_data) => {
            function flatten(obj) {
                let attrs = obj.attributes || {},
                    stats = attrs.stats || {},
                    o = Object.assign({}, obj, attrs, stats);
                delete o.type;
                delete o.attributes;
                delete o.stats;
                delete o.relationships;
                return o;
            }
            let match = JSON.parse(JSON.stringify(match_data));  // deep clone

            /* bring jsonapi response into our db structure-like shape */
            match.rosters = match.rosters.map((roster) => {
                roster.participants = roster.participants.map((participant) => {
                    participant.player = flatten(participant.player);
                    return flatten(participant);
                });
                return flatten(roster);
            });
            match = flatten(match);

            /* upsert everything */
            model.Match.upsert(match, {
                include: [
                    {
                        model: model.Roster,
                        as: "Rosters",
                        include: [
                            {
                                model: model.Participant,
                                as: "Participants",
                                include: [
                                    model.Player
                                ]
                            },
                            {
                                model: model.Team,
                                as: "Team"
                            }
                        ]
                    },
                    {
                        model: model.Asset,
                        as: "Assets"
                    }
                ]
            });

            match.rosters.forEach((roster) => {
                model.Roster.upsert(roster, {
                    include: [
                        {
                            model: model.Participant,
                            as: "Participants",
                            include: [
                                model.Player
                            ]
                        },
                        {
                            model: model.Team,
                            as: "Team"
                        }
                    ]
                });

                roster.participants.forEach((participant) => {
                    model.Participant.upsert(participant, {
                        include: [
                            model.Player
                        ]
                    });

                    model.Player.upsert(participant.player);
                });

                if (roster.team != null) model.Team.upsert(roster.team);
            });

            match.assets.forEach((asset) => {
                model.Asset.upsert(asset);
            });
        });

        await transaction.commit();  // TODO rollback on err
        ch.ack(msg);
    }, { noAck: false });
})();
