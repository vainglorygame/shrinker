/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('match', {
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
    shard_id: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    series: {
      type: DataTypes.STRING(191),
      allowNull: true
    },
    created_at: {
      type: DataTypes.TIME,
      allowNull: false
    },
    duration: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    game_mode: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    end_game_reason: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    queue: {
      type: DataTypes.STRING(191),
      allowNull: false
    }
  }, {
    tableName: 'match',
    timestamps: false,
    underscored: true,
    freezeTableName: true
  });
};
