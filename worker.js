#!/usr/bin/node
/* jshint esnext:true */
'use strict';

var amqp = require("amqplib"),
    Seq = require("sequelize");

var RABBITMQ_URI = process.env.RABBITMQ_URI || "amqp://localhost",
    DATABASE_URI = process.env.DATABASE_URI || "sqlite:///db.sqlite";

(async () => {
    let seq = new Seq(DATABASE_URI),
        model = require("./model")(seq, Seq),
        rabbit = await amqp.connect(RABBITMQ_URI),
        ch = await rabbit.createChannel();

    /* recreate for debugging
    await seq.query("SET FOREIGN_KEY_CHECKS=0");
    await seq.sync({force: true});
    */
    //await seq.sync();

    await ch.assertQueue("process", {durable: true});
    await ch.prefetch(1);

    ch.consume("process", async (msg) => {
        let match = JSON.parse(msg.content);

        // TODO commit less often if possible, avoid deadlocks
        let transaction = await seq.transaction({ autocommit: false });

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

        /* bring jsonapi nested response into our db structure-like shape */
        match.rosters = match.rosters.map((roster) => {
            roster.participants = roster.participants.map((participant) => {
                participant.player = flatten(participant.player);
                return flatten(participant);
            });
            return flatten(roster);
        });
        match = flatten(match);
        console.log(match);

        /* upsert everything */
        await match.rosters.forEach(async (roster) => {
            await roster.participants.forEach(async (participant) => {
                await model.Player.upsert(participant.player);

                participant.roster_api_id = roster.api_id;
                participant.player_api_id = participant.player.api_id;
                await model.Participant.upsert(participant, {
                    include: [ model.Player ]
                });
            });

            //if (roster.team != null) model.Team.upsert(roster.team);

            roster.match_api_id = match.api_id;
            await model.Roster.upsert(roster, {
                include: [ model.Participant/*, model.Team*/ ]
            });
        });

        /*match.assets.forEach((asset) => {
            model.Asset.upsert(asset);
        });*/

        await model.Match.upsert(match, {
            include: [ model.Roster/*, model.Asset*/ ]
        });

        await transaction.commit();  // TODO rollback on err
        ch.ack(msg);
    }, { noAck: false });
})();
