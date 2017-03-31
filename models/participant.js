/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('participant', {
    id: {
      type: DataTypes.INTEGER(10).UNSIGNED,
      allowNull: false,
      primaryKey: true,
      autoIncrement: true
    },
    shard_id: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    api_id: {
      type: DataTypes.STRING(191),
      allowNull: false,
      unique: true
    },
    player_api_id: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    roster_api_id: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    created_at: {
      type: DataTypes.TIME,
      allowNull: true
    },
    series: {
      type: DataTypes.TIME,
      allowNull: true
    },
    actor: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    assists: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    crystal_mine_captures: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    deaths: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    farm: {
      type: "DOUBLE",
      allowNull: false
    },
    first_afk_time: {
      type: "DOUBLE",
      allowNull: false
    },
    gold: {
      type: DataTypes.INTEGER(11),
      allowNull: true
    },
    gold_mine_captures: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    jungle_kills: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    karma_level: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    kills: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    kraken_captures: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    level: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    minion_kills: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    non_jungle_minion_kills: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    skill_tier: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    skin_key: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    turret_captures: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    went_afk: {
      type: DataTypes.INTEGER(1),
      allowNull: false
    },
    winner: {
      type: DataTypes.INTEGER(1),
      allowNull: false
    }
  }, {
    tableName: 'participant',
    timestamps: false,
    underscored: true,
    freezeTableName: true
  });
};
