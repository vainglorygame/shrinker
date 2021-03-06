#!/usr/bin/node
/* jshint esnext:true */
"use strict";

/* shrinker aggregates Telemetry data and inserts it into the database. */

const amqp = require("amqplib"),
    Promise = require("bluebird"),
    winston = require("winston"),
    loggly = require("winston-loggly-bulk"),
    datadog = require("winston-datadog"),
    Seq = require("sequelize"),
    cacheManager = require("cache-manager"),
    api_name_mappings = require("../orm/mappings").map,
    crashIfBullshit = require("../orm/mappings").crashIfBullshit,
    isAbility = require("../orm/mappings").isAbility;

const RABBITMQ_URI = process.env.RABBITMQ_URI,
    DATABASE_URI = process.env.DATABASE_URI,
    QUEUE = process.env.QUEUE || "shrink",
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN,
    DATADOG_TOKEN = process.env.DATADOG_TOKEN,
    // matches + players, 5 players with 50 matches as default
    BATCHSIZE = parseInt(process.env.BATCHSIZE) || 20,
    // maximum number of elements to be inserted in one statement
    CHUNKSIZE = parseInt(process.env.CHUNKSIZE) || 100,
    MAXCONNS = parseInt(process.env.MAXCONNS) || 1,  // how many concurrent actions
    DOREAPMATCH = process.env.DOREAPMATCH == "true",
    REAP_QUEUE = process.env.REAP_QUEUE || "reap",
    LOAD_TIMEOUT = parseFloat(process.env.LOAD_TIMEOUT) || 5000, // ms
    IDLE_TIMEOUT = parseFloat(process.env.IDLE_TIMEOUT) || 700;  // ms

const logger = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({
            timestamp: true,
            colorize: true
        }),
        new (winston.transports.File)({
            label: QUEUE,
            filename: "shrinker.log"
        })
    ]
});

// loggly integration
if (LOGGLY_TOKEN)
    logger.add(winston.transports.Loggly, {
        inputToken: LOGGLY_TOKEN,
        subdomain: "kvahuja",
        tags: ["backend", "shrinker", QUEUE],
        json: true
    });

// datadog integration
if (DATADOG_TOKEN)
    logger.add(new datadog({
        api_key: DATADOG_TOKEN
    }), null, true);

// split an array into arrays of max chunksize
function* chunks(arr) {
    for (let c=0, len=arr.length; c<len; c+=CHUNKSIZE)
        yield arr.slice(c, c+CHUNKSIZE);
}

// MariaDB doesn't accept `COLUMN_CREATE()`
// so this is a helper to return either Seq expression or ""
function dynamicColumn(arr) {
    if (arr.length == 0)
        return "";
    else
        return Seq.fn("COLUMN_CREATE", arr);
}

amqp.connect(RABBITMQ_URI).then(async (rabbit) => {
    global.process.on("SIGINT", () => {
        rabbit.close();
        global.process.exit();
    });

    // connect to rabbit & db
    const seq = new Seq(DATABASE_URI, {
        logging: false,
        pool: {
            max: MAXCONNS
        }
    });

    const cache = cacheManager.caching({store: "memory", max: 1000, ttl: 60 });

    const ch = await rabbit.createChannel();
    await ch.assertQueue(QUEUE, { durable: true });
    await ch.assertQueue(QUEUE + "_failed", { durable: true });
    await ch.assertQueue(REAP_QUEUE, { durable: true });
    // as long as the queue is filled, msg are not ACKed
    // server sends as long as there are less than `prefetch` unACKed
    await ch.prefetch(BATCHSIZE);

    logger.info("configuration", {
        QUEUE, BATCHSIZE, CHUNKSIZE, MAXCONNS, LOAD_TIMEOUT, IDLE_TIMEOUT,
        DOREAPMATCH, REAP_QUEUE
    });

    const model = require("../orm/model")(seq, Seq);

    // performance logging
    let load_timer = undefined,
        idle_timer = undefined,
        profiler = undefined;

    // Maps to quickly convert API names to db ids
    let item_db_map = new Map(),      // "Halcyon Potion" to id
        hero_db_map = new Map();      // "*SAW*" to id

    // populate maps
    await Promise.all([
        model.Item.findAll()
            .map((item) => item_db_map.set(item.name, item.id)),
        model.Hero.findAll()
            .map((hero) => hero_db_map.set(hero.name, hero.id))
    ]);
    if (item_db_map.size == 0 ||
        hero_db_map.size == 0) {
        logger.error("mapping tables are not seeded!!! quitting");
        global.process.exit();
    }

    // buffers that will be filled until BATCHSIZE is reached
    // to make db transactions more efficient
    let telemetry_data = new Set(),
        msg_buffer = new Set();

    ch.consume(QUEUE, async (msg) => {
        telemetry_data.add(JSON.parse(msg.content));
        msg_buffer.add(msg);

        // fill queue until batchsize or idle
        // for logging of the time between batch fill and batch process
        if (profiler == undefined) profiler = logger.startTimer();
        // timeout after first job
        if (load_timer == undefined)
            load_timer = setTimeout(tryProcess, LOAD_TIMEOUT);
        // timeout after last job
        if (idle_timer != undefined)
            clearTimeout(idle_timer);
        idle_timer = setTimeout(tryProcess, IDLE_TIMEOUT);
        // maximum data pressure
        if (telemetry_data.size == BATCHSIZE)
            await tryProcess();
    }, { noAck: false });

    // wrap process() in message handler
    async function tryProcess() {
        profiler.done("buffer filled");
        profiler = undefined;

        logger.info("processing batch", {
            telemetries: telemetry_data.size
        });

        const msgs = new Set(msg_buffer);
        msg_buffer.clear();
        const telemetry_objects = new Set(telemetry_data);
        telemetry_data.clear();

        // clean up to allow processor to accept while we wait for db
        clearTimeout(idle_timer);
        clearTimeout(load_timer);

        idle_timer = undefined;
        load_timer = undefined;

        try {
            await process(telemetry_objects);
        } catch (err) {
            if (err instanceof Seq.TimeoutError) {
                // deadlocks / timeout
                logger.error("SQL error", err);
                await Promise.map(msgs, async (m) =>
                    await ch.nack(m, false, true));  // retry
            } else {
                // log, move to error queue and NACK
                logger.error(err);
                await Promise.map(msgs, async (m) => {
                    await ch.sendToQueue(QUEUE + "_failed", m.content, {
                        persistent: true,
                        headers: m.properties.headers
                    });
                    await ch.nack(m, false, false);
                });
            }
            return;
        }

        logger.info("acking batch", { size: msgs.size });
        await Promise.map(msgs, async (m) => await ch.ack(m));
        // notify web
        await Promise.map(msgs, async (m) => {
            if (m.properties.headers.notify == undefined) return;
            // new phases
            // notify match.api_id about phase_update
            await ch.publish("amq.topic",
                m.properties.headers.notify,
                new Buffer("phase_update"))
        });
        if (DOREAPMATCH)
            await Promise.map(telemetry_objects, async (t) =>
                await ch.sendToQueue(REAP_QUEUE, new Buffer(
                    JSON.stringify({
                        match_api_id: t.match_api_id,
                        start: t.start < 0? 0 : t.start,  // TODO see below
                        end: t.end
                    }), { persistent: true })
                )
            );
    }

    // finish a whole batch
    async function process(telemetry_objects) {
        // aggregate record objects to do a bulk insert
        let participant_phase_records = [];

        // data from Telemetry, one phase (early/mid/late/…) per obj
        await Promise.map(telemetry_objects, async (telemetry) => {
            let dbpreload_profiler = logger.startTimer();

            // api -> telemetry format
            const sideToTeam = (s) => s == "left/blue"? "Left" : "Right",
                // yes there is yet another format and yes it's strings
                sideToTeamNo = (s) => s == "left/blue"? "1" : "2";
            // get match participant references
            const participants =
                (await cache.wrap(telemetry.match_api_id, async () =>
                    await model.Participant.findAll({
                        where: { match_api_id: telemetry.match_api_id },
                        attributes: [ "api_id", "player_api_id", "actor" ],
                        include: [ {  // TODO rm once pushed to participant
                            model: model.Roster,
                            attributes: [ "side" ]
                        } ]
                    })
                ) ).map((p) => { return {
                    api_id: p.api_id,
                    player_api_id: p.player_api_id,
                    actor: p.actor,
                    team: sideToTeam(p.roster.side),
                    teamNo: sideToTeamNo(p.roster.side)
                } });

            dbpreload_profiler.done("loading relationships from db");

            // link participant <-> Telemetry actor/target
            // available as `.actor` or as `.target`
            telemetry.data.forEach((t) => {
                // patch schema, Delt -> Dealt from 2.9 onwards
                // see https://github.com/gamelocker/vainglory-assets/pull/308
                t.payload.Dealt = t.payload.Dealt || t.payload.Delt;
                if (isNaN(t.payload.Dealt)) {
                    t.payload.Dealt = 0;
                }

                // linking
                if (t.type == "HeroSelect")
                    t.actor = participants.filter((p) =>
                        p.player_api_id == t.payload.Player)[0];
                if (t.type == "BuyItem"
                    || t.type == "SellItem"
                    || t.type == "UseItemAbility"
                    || t.type == "LearnAbility"
                    || t.type == "UseAbility"
                    || t.type == "LevelUp")
                    t.actor = participants.filter((p) =>
                        p.actor == t.payload.Actor
                        && p.team == t.payload.Team)[0];
                if (t.type == "UseItemAbility"
                    || t.type == "UseAbility")
                    t.target = participants.filter((p) =>
                        p.actor == t.payload.TargetActor
                        && p.team != t.payload.Team)[0];
                // damage actor
                if ((t.type == "DealDamage"
                     || t.type == "KillActor")
                    && t.payload.IsHero == 1)
                    t.actor = participants.filter((p) =>
                        p.actor == t.payload.Actor
                        && p.team == t.payload.Team)[0];
                // damage target
                if (t.type == "DealDamage"
                    && t.payload.TargetIsHero == 1)
                    t.target = participants.filter((p) =>
                        p.actor == t.payload.Target
                        && p.team != t.payload.Team)[0];
                // kill target
                if (t.type == "KillActor"
                    && t.payload.TargetIsHero == 1)
                    t.target = participants.filter((p) =>
                        p.actor == t.payload.Killed
                        && p.team == t.payload.KilledTeam)[0];
                // heal actor (since 2.9)
                if (t.type == "HealTarget"
                    && t.payload.IsHero == 1
                    && ["Buff_SpawnStage_Recharge", "Buff_Ace"].indexOf(t.payload.Source) == -1  // TODO LOL what?
                    && t.payload.Team == t.payload.TargetTeam)  // enemies "heal" Ardan due to his perk WTF
                    t.actor = participants.filter((p) =>
                        p.actor == t.payload.Actor
                        && p.team == t.payload.Team)[0];
                // heal target (since 2.9)
                if (t.type == "HealTarget"
                    && t.payload.TargetIsHero == 1
                    && ["Buff_SpawnStage_Recharge", "Buff_Ace"].indexOf(t.payload.Source) == -1)
                    t.target = participants.filter((p) =>
                        p.actor == t.payload.TargetActor
                        && p.team == t.payload.TargetTeam)[0];
                // vampirism actor (since 2.10)
                if (t.type == "Vampirism"
                    && t.payload.IsHero == 1)
                    t.actor = participants.filter((p) =>
                        p.actor == t.payload.Actor
                        && p.team == t.payload.Team)[0];
                // vampirism target (since 2.10)
                if (t.type == "Vampirism"
                    && t.payload.TargetIsHero == 1)
                    t.target = participants.filter((p) =>
                        p.actor == t.payload.TargetActor
                        && p.team == t.payload.TargetTeam)[0];
            });

            const participants_phase = participants.map((p) => { return {
                // TODO ban data workaround
                start: telemetry.start < 0? 0 : telemetry.start,  // in seconds
                end: telemetry.end,
                participant_api_id: p.api_id,
                match_api_id: telemetry.match_api_id,

                dmg_true: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                dmg_dealt: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    ? acc + ev.payload.Dealt
                    : acc
                , 0),
                dmg_rcvd_dealt: telemetry.data.reduce((acc, ev) =>
                    ev.target == p
                    && ev.type == "DealDamage"
                    ? acc + ev.payload.Dealt
                    : acc
                , 0),
                dmg_rcvd_true: telemetry.data.reduce((acc, ev) =>
                    ev.target == p
                    && ev.type == "DealDamage"
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                draft_position: telemetry.data.filter((ev) =>
                    ev.type == "HeroSelect").indexOf(
                        telemetry.data.filter((ev) =>
                            ev.type == "HeroSelect"
                            && ev.actor == p)[0]),
                ban: hero_db_map.get(api_name_mappings.get(
                    telemetry.data
                        .filter((ev) => ev.type == "HeroBan" &&
                            ev.payload.Team == p.teamNo)
                        .map((sel) => sel.payload.Hero)[0]  // can be null
                )),
                pick: hero_db_map.get(api_name_mappings.get(
                    telemetry.data
                        .filter((ev) =>
                            ev.type == "HeroSelect"
                            && ev.actor == p)
                        .map((sel) => sel.payload.Hero)[0]  // can be null
                )),
                // since 2.9
                heal_heal: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "HealTarget"
                    ? acc + ev.payload.Heal
                    : acc
                , 0),
                // actual heal from actor to hero
                heal_healed: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "HealTarget"
                    ? acc + ev.payload.Healed
                    : acc
                , 0),
                // theoretical heal received from hero
                heal_rcvd_heal: telemetry.data.reduce((acc, ev) =>
                    ev.target == p
                    && ev.type == "HealTarget"
                    ? acc + ev.payload.Heal
                    : acc
                , 0),
                // actual heal received from hero
                heal_rcvd_healed: telemetry.data.reduce((acc, ev) =>
                    ev.target == p
                    && ev.type == "HealTarget"
                    ? acc + ev.payload.Healed
                    : acc
                , 0),
                // lifesteal
                heal_rcvd_vamp: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "Vampirism"
                    ? acc + ev.payload.Vamp
                    : acc
                , 0)
            } });
            participant_phase_records = participant_phase_records.concat(
                participants_phase);
        }, { concurrency: MAXCONNS });

        let transaction_profiler = logger.startTimer();
        // now access db
        // upsert whole batch in parallel
        logger.info("inserting batch into db");
        await seq.transaction({ autocommit: false }, async (transaction) => {
            await Promise.map(chunks(participant_phase_records), async (p_p_r) =>
                model.ParticipantPhases.bulkCreate(p_p_r, {
                    ignoreDuplicates: true,
                    //updateOnDuplicate: [],
                    transaction: transaction
                }), { concurrency: MAXCONNS }
            );
        });
        transaction_profiler.done("database transaction");
    }
});

process.on("unhandledRejection", (err) => {
    logger.error(err);
    process.exit(1);  // fail hard and die
});
