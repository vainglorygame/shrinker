/* jshint esnext:true */
'use strict';

var JsonField = require("sequelize-json");

module.exports = (seq, Seq) => {
    let Match = seq.import("./models/match.js"),
        Asset = seq.import("./models/asset.js"),
        Roster = seq.import("./models/roster.js"),
        //Team = seq.import("./models/team.js"),
        Participant = seq.import("./models/participant.js"),
        ParticipantItemUse = seq.import("./models/participant_item_use.js"),
        Player = seq.import("./models/player.js"),
        Item = seq.import("./models/item.js");

    Match.hasMany(Asset, {as: "Assets"});
    Asset.belongsTo(Match);
    Roster.belongsTo(Match, { foreignKey: "match_api_id", targetKey: "api_id" });
    //Roster.hasOne(Team, {as: "Team"});
    //Team.belongsTo(Roster);
    Participant.belongsTo(Roster, { foreignKey: "roster_api_id", targetKey: "api_id" });
    Participant.hasOne(Player, { foreignKey: "player_api_id", targetKey: "api_id" });
    ParticipantItemUse.belongsTo(Participant, { foreignKey: "participant_api_id", targetKey: "api_id" });
    ParticipantItemUse.hasOne(Item);

    //return {Match, Roster, Team, Participant, Player, Asset};
    return {Match, Roster, Participant, ParticipantItemUse, Player, Asset, Item};
};
