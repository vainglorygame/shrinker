/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('player', {
    id: {
      type: DataTypes.INTEGER(10).UNSIGNED,
      allowNull: false,
      primaryKey: true,
      autoIncrement: true
    },
    api_id: {
      type: DataTypes.STRING(191),
      allowNull: false,
      unique: true
    },
    created_at: {
      type: DataTypes.TIME,
      allowNull: true
    },
    name: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    shard_id: {
      type: DataTypes.STRING(191),
      allowNull: true
    },
    last_match_created_date: {
      type: DataTypes.TIME,
      allowNull: true
    },
    level: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    xp: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    lifetime_gold: {
      type: "DOUBLE(8,2)",
      allowNull: false
    }
  }, {
    tableName: 'player'
  });
};
