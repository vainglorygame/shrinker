/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('match_ext', {
    id: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    match_api_id: {
      type: DataTypes.STRING(191),
      allowNull: false,
      unique: true
    },
    winning_side: {
      type: DataTypes.STRING(191),
      allowNull: true
    },
    total_kills: {
      type: DataTypes.INTEGER(11),
      allowNull: true
    },
    kraken_captures: {
      type: DataTypes.INTEGER(11),
      allowNull: true
    },
    gold: {
      type: DataTypes.INTEGER(11),
      allowNull: true
    },
    aces_earned: {
      type: DataTypes.INTEGER(11),
      allowNull: true
    },
    turret_captures: {
      type: DataTypes.INTEGER(11),
      allowNull: true
    },
    gold_mine_captures: {
      type: DataTypes.INTEGER(11),
      allowNull: true
    },
    crystal_miner_captures: {
      type: DataTypes.INTEGER(11),
      allowNull: true
    }
  }, {
    tableName: 'match_ext',
    timestamps: false,
    underscored: true,
    freezeTableName: true
  });
};
