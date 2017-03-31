/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('roster_ext', {
    id: {
      type: DataTypes.INTEGER(10).UNSIGNED,
      allowNull: false,
      primaryKey: true,
      autoIncrement: true
    },
    roster_api_id: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    kills: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    assists: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    deaths: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    gold: {
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
    turret_captures: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    turrets_remaining: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    }
  }, {
    tableName: 'roster_ext',
    timestamps: false,
    underscored: true,
    freezeTableName: true
  });
};
