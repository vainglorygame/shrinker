/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('roster', {
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
    match_api_id: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    aces_earned: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    gold: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    hero_kills: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    kraken_captures: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    side: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    turret_kills: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    turrets_remaining: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    }
  }, {
    tableName: 'roster',
    timestamps: false,
    underscored: true,
    freezeTableName: true
  });
};
