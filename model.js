/* jshint esnext:true */
'use strict';

var JsonField = require("sequelize-json");

module.exports = (seq, Seq) => {
    let Player = seq.define("player", {
        id: { type: Seq.STRING, unique: true, primaryKey: true },
        /* attributes */
        name: Seq.STRING,
        shardId: Seq.STRING,
        /* stats */
        level: Seq.INTEGER,
        lifetimeGold: Seq.DECIMAL,
        lossStreak: Seq.INTEGER,
        played: Seq.INTEGER,
        played_ranked: Seq.INTEGER,
        winStreak: Seq.INTEGER,
        wins: Seq.INTEGER,
        xp: Seq.INTEGER
    }, {
        freezeTableName: true,
        underscored: true
    });
    let Team = seq.define("team", {
        id: { type: Seq.STRING, unique: true, primaryKey: true },
        /* attributes */
        name: Seq.STRING,
        shardId: Seq.STRING
        /* stats */
    }, {
        freezeTableName: true,
        underscored: true
    });
    let Asset = seq.define("asset", {
        id: { type: Seq.STRING, unique: true, primaryKey: true },
        /* attributes */
        URL: Seq.STRING,
        contentType: Seq.STRING,
        createdAt: Seq.DATE,
        description: Seq.TEXT,
        filename: Seq.STRING,
        name: Seq.STRING
    }, {
        freezeTableName: true,
        underscored: true,
        createdAt: false
    });
    let Match = seq.define("match", {
        id: { type: Seq.STRING, unique: true, primaryKey: true },
        /* attributes */
        createdAt: Seq.DATE,
        duration: Seq.INTEGER,
        gameMode: Seq.STRING,
        patchVersion: Seq.STRING,
        shardId: Seq.STRING,
        /* stats */
        endGameReason: Seq.STRING,
        queue: Seq.STRING
    }, {
        freezeTableName: true,
        underscored: true,
        createdAt: false
    });
    let Roster = seq.define("roster", {
        id: { type: Seq.STRING, unique: true, primaryKey: true },
        /* attributes */
        /* stats */
        acesEarned: Seq.INTEGER,
        gold: Seq.INTEGER,
        heroKills: Seq.INTEGER,
        krakenCaptures: Seq.INTEGER,
        side: Seq.STRING,
        turretKills: Seq.INTEGER,
        turretsRemaining: Seq.INTEGER,
    }, {
        freezeTableName: true,
        underscored: true
    });
    let Participant = seq.define("participant", {
        id: { type: Seq.STRING, unique: true, primaryKey: true },
        /* attributes */
        actor: Seq.STRING,
        /* stats */
        assists: Seq.INTEGER,
        crystalMineCaptures: Seq.INTEGER,
        deaths: Seq.INTEGER,
        farm: Seq.DECIMAL,
        firstAfkTime: Seq.INTEGER,
        goldMineCaptures: Seq.INTEGER,
        itemGrants: JsonField(seq, "Participant", "itemGrants"),
        itemSells: JsonField(seq, "Participant", "itemSells"),
        itemUses: JsonField(seq, "Participant", "itemUses"),
        items: JsonField(seq, "Participant", "items"),
        jungleKills: Seq.INTEGER,
        karmaLevel: Seq.INTEGER,
        kills: Seq.INTEGER,
        krakenCaptures: Seq.INTEGER,
        level: Seq.INTEGER,
        minionKills: Seq.INTEGER,
        nonJungleMinionKills: Seq.INTEGER,
        skillTier: Seq.INTEGER,
        skinKey: Seq.STRING,
        turretCaptures: Seq.INTEGER,
        wentAfk: Seq.BOOLEAN,
        winner: Seq.BOOLEAN,
    }, {
        freezeTableName: true,
        underscored: true
    });

    Match.hasMany(Roster, {as: "Rosters"});
    Match.hasMany(Asset, {as: "Assets"});
    Asset.belongsTo(Match);
    Roster.belongsTo(Match);
    Roster.hasMany(Participant, {as: "Participants"});
    Roster.hasOne(Team, {as: "Team"});
    Team.belongsTo(Roster);
    Participant.belongsTo(Roster);
    Participant.belongsTo(Player);

    return {Match, Roster, Team, Participant, Player, Asset};
};
