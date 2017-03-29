/* jshint esnext:true */
'use strict';

var JsonField = require("sequelize-json");

module.exports = (seq, Seq) => {
    let Match = seq.import("./models/match.js"),
        Roster = seq.import("./models/roster.js"),
        Participant = seq.import("./models/participant.js"),
        Player = seq.import("./models/player.js");

    //Match.hasMany(Asset, {as: "Assets"});
    //Asset.belongsTo(Match);
    Roster.belongsTo(Match, { foreignKey: "match_api_id", targetKey: "api_id" });
    //Roster.hasOne(Team, {as: "Team"});
    //Team.belongsTo(Roster);
    Participant.belongsTo(Roster, { foreignKey: "roster_api_id", targetKey: "api_id" });
    Participant.hasOne(Player, { foreignKey: "player_api_id", targetKey: "api_id" });

    //return {Match, Roster, Team, Participant, Player, Asset};
    return {Match, Roster, Participant, Player};
};
