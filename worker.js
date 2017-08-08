#!/usr/bin/node
/* jshint esnext:true */
"use strict";

/* shrinker aggregates Telemetry data and inserts it into the database. */

const amqp = require("amqplib"),
    Promise = require("bluebird"),
    winston = require("winston"),
    loggly = require("winston-loggly-bulk"),
    Seq = require("sequelize"),
    api_name_mappings = require("../orm/mappings").map,
    isAbility = require("../orm/mappings").isAbility;

const RABBITMQ_URI = process.env.RABBITMQ_URI,
    DATABASE_URI = process.env.DATABASE_URI,
    QUEUE = process.env.QUEUE || "process",
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN,
    // matches + players, 5 players with 50 matches as default
    BATCHSIZE = parseInt(process.env.BATCHSIZE) || 5 * (50 + 1),
    // maximum number of elements to be inserted in one statement
    CHUNKSIZE = parseInt(process.env.CHUNKSIZE) || 100,
    MAXCONNS = parseInt(process.env.MAXCONNS) || 10,  // how many concurrent actions
    DOANALYZEMATCH = process.env.DOANALYZEMATCH == "true",
    ANALYZE_QUEUE = process.env.ANALYZE_QUEUE || "analyze",
    LOAD_TIMEOUT = parseFloat(process.env.LOAD_TIMEOUT) || 5000, // ms
    IDLE_TIMEOUT = parseFloat(process.env.IDLE_TIMEOUT) || 700;  // ms

const logger = new (winston.Logger)({
    transports: [
        new (winston.transports.Console)({
            timestamp: true,
            colorize: true
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

// split an array into arrays of max chunksize
function* chunks(arr) {
    for (let c=0, len=arr.length; c<len; c+=CHUNKSIZE)
        yield arr.slice(c, c+CHUNKSIZE);
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

    const ch = await rabbit.createChannel();
    await ch.assertQueue(QUEUE, { durable: true });
    await ch.assertQueue(QUEUE + "_failed", { durable: true });
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
        process.exit();
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
    }

    // finish a whole batch
    async function process(telemetry_objects) {
        // aggregate record objects to do a bulk insert
        let participant_phase_records = [];

        // data from Telemetry, one phase (early/mid/late/…) per obj
        await Promise.map(telemetry_objects, async (telemetry) => {
            // api -> telemetry format
            const sideToTeam = (s) => s == "left/blue"? "Left" : "Right",
                // yes there is yet another format and yes it's strings
                sideToTeamNo = (s) => s == "left/blue"? "1" : "2";
            // get match participant references
            const participants = await model.Participant.findAll({
                where: { match_api_id: telemetry.match_api_id },
                include: [ {  // TODO rm once pushed to participant
                    model: model.Roster,
                    attributes: [ "side" ]
                } ]
            }).map((p) => { return {
                api_id: p.api_id,
                player_api_id: p.player_api_id,
                actor: p.actor,
                team: sideToTeam(p.roster.side),
                teamNo: sideToTeamNo(p.roster.side)
            } });

            // seconds since epoch; first spawn time
            const matchstart = new Date(Date.parse(telemetry.match_start)).getTime() / 1000;

            // link participant <-> Telemetry actor/target
            // available as `.actor` or as `.target`
            telemetry.data.forEach((t) => {
                // seconds after this phase's start
                t.offset = new Date(Date.parse(t.time)).getTime() / 1000 - matchstart;

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
            });

            const participants_phase = participants.map((p) => { return {
                // TODO ban data workaround
                start: telemetry.start < 0? 0 : telemetry.start,  // in seconds
                end: telemetry.end,
                participant_api_id: p.api_id,

                kills: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && ev.payload.IsHero == 1
                    && ev.payload.TargetIsHero == 1
                ).length,
                deaths: telemetry.data.filter((ev) =>
                    ev.target == p
                    && ev.type == "KillActor"
                    && ev.payload.TargetIsHero == 1
                ).length,
                // assists missing in data
                minion_kills: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && ["*JungleMinion_TreeEnt*",
                        "*Neutral_JungleMinion_DefaultBig*",
                        "*Neutral_JungleMinion_DefaultSmall*",
                        "*LeadMinion*",
                        "*RangedMinion*",
                        "*TankMinion*"
                    ].indexOf(ev.payload.Killed) != -1
                ).length,
                jungle_kills: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && ["*JungleMinion_TreeEnt*",
                        "*Neutral_JungleMinion_DefaultBig*",
                        "*Neutral_JungleMinion_DefaultSmall*"
                    ].indexOf(ev.payload.Killed) != -1
                ).length,
                non_jungle_minion_kills: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && ["*LeadMinion*",
                        "*RangedMinion*",
                        "*TankMinion*"
                    ].indexOf(ev.payload.Killed) != -1
                ).length,
                crystal_mine_captures: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && ev.payload.Killed == "*JungleMinion_CrystalMiner*"
                ).length,
                gold_mine_captures: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && ev.payload.Killed == "*JungleMinion_GoldMiner*"
                ).length,
                kraken_captures: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && ev.payload.Killed == "*Kraken_Jungle*"
                ).length,
                turret_captures: telemetry.data.filter((ev) =>
                    ev.actor == p
                    && ev.type == "KillActor"
                    && (ev.payload.Killed == "*Turret*"
                        || ev.payload.Killed == "*VainTurret*")
                ).length,
                // TODO Telemetry does not give accurate LifetimeGold
                gold: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "LevelUp"
                    && ev.payload.LifetimeGold > acc
                    ? ev.payload.LifetimeGold
                    : acc
                , null),
                dmg_true_hero: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.TargetIsHero == 1
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                dmg_true_kraken: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ["*Kraken_Jungle*",
                        "*Kraken_Captured*"
                    ].indexOf(ev.payload.Target) != -1
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                dmg_true_turret: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.Target == "*Turret*"
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                dmg_true_vain_turret: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.Target == "*VainTurret*"
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                dmg_true_others: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.TargetIsHero == 0
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                dmg_dealt_hero: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.TargetIsHero == 1
                    ? acc + ev.payload.Delt
                    : acc
                , 0),
                dmg_dealt_kraken: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ["*Kraken_Jungle*",
                        "*Kraken_Captured*"
                    ].indexOf(ev.payload.Target) != -1
                    ? acc + ev.payload.Delt
                    : acc
                , 0),
                dmg_dealt_turret: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.Target == "*Turret*"
                    ? acc + ev.payload.Delt
                    : acc
                , 0),
                dmg_dealt_vain_turret: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.Target == "*VainTurret*"
                    ? acc + ev.payload.Delt
                    : acc
                , 0),
                dmg_dealt_others: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.TargetIsHero == 0
                    ? acc + ev.payload.Delt
                    : acc
                , 0),
                dmg_rcvd_dealt_hero: telemetry.data.reduce((acc, ev) =>
                    ev.target == p
                    && ev.type == "DealDamage"
                    && ev.payload.IsHero == 1
                    ? acc + ev.payload.Delt
                    : acc
                , 0),
                dmg_rcvd_true_hero: telemetry.data.reduce((acc, ev) =>
                    ev.target == p
                    && ev.type == "DealDamage"
                    && ev.payload.IsHero == 1
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                dmg_rcvd_dealt_others: telemetry.data.reduce((acc, ev) =>
                    ev.target == p
                    && ev.type == "DealDamage"
                    && ev.payload.IsHero == 0
                    ? acc + ev.payload.Delt
                    : acc
                , 0),
                dmg_rcvd_true_others: telemetry.data.reduce((acc, ev) =>
                    ev.target == p
                    && ev.type == "DealDamage"
                    && ev.payload.IsHero == 0
                    ? acc + ev.payload.Damage
                    : acc
                , 0),
                hero_level: telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "LevelUp"
                    && ev.payload.Level > acc
                    ? ev.payload.Level
                    : acc
                , -1),
                items: null,  // TODO
                item_grants: JSON.stringify(telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "BuyItem"
                    ? acc.concat(item_db_map.get(api_name_mappings.get(ev.payload.Item)))
                    : acc
                , [])),
                item_sells: JSON.stringify(telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "SellItem"
                    ? acc.concat(item_db_map.get(api_name_mappings.get(ev.payload.Item)))
                    : acc
                , [])),
                ability_levels: JSON.stringify(telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "LearnAbility"
                    ? acc.concat([ [ api_name_mappings.get(ev.payload.Ability).split(" ")[1],
                        ev.offset ] ])
                    : acc
                , [])),
                ability_uses: JSON.stringify(telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "UseAbility"
                    && ["A", "B", "C"].indexOf(
                        api_name_mappings.get(ev.payload.Ability).split(" ")[1]
                    ) != -1
                    ? acc.concat([ [ api_name_mappings.get(ev.payload.Ability).split(" ")[1],
                        ev.offset ] ])
                    : acc
                , [])),
                ability_damage: JSON.stringify(telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "DealDamage"
                    && ev.payload.IsHero == 1
                    && isAbility(ev.payload.Source)  // TODO
                    && api_name_mappings.has(ev.payload.Source)
                    && ["A", "B", "C"].indexOf(
                        api_name_mappings.get(ev.payload.Source).split(" ")[1]
                    ) != -1  // TODO refactor here
                    ? acc.concat([ [ api_name_mappings.get(ev.payload.Source).split(" ")[1],
                          ev.payload.Damage, ev.offset ] ])
                    : acc
                , [])),
                item_uses: JSON.stringify(telemetry.data.reduce((acc, ev) =>
                    ev.actor == p
                    && ev.type == "UseItemAbility"
                    ? acc.concat([ [ item_db_map.get(api_name_mappings.get(ev.payload.Ability)),
                          ev.offset ] ])
                    : acc
                , [])),
                player_damage: null,  // TODO
                items: null,  // TODO
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
                )),// traits calculated later
            } });
            participant_phase_records = participant_phase_records.concat(
                participants_phase);  // TODO calc stats
        });

        let transaction_profiler = logger.startTimer();
        // now access db
        // upsert whole batch in parallel
        logger.info("inserting batch into db");
        await seq.transaction({ autocommit: false }, async (transaction) => {
            await Promise.map(chunks(participant_phase_records), async (p_p_r) =>
                model.ParticipantPhases.bulkCreate(p_p_r, {
                    ignoreDuplicates: true,
                    updateOnDuplicate: [],
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
