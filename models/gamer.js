/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('gamer', {
    id: {
      type: DataTypes.INTEGER(10).UNSIGNED,
      allowNull: false,
      primaryKey: true,
      autoIncrement: true
    },
    player_api_id: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    name: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    vainsocial_status: {
      type: DataTypes.STRING(191),
      allowNull: true
    },
    vainglory_ign: {
      type: DataTypes.STRING(191),
      allowNull: true
    },
    vainglory_shard_id: {
      type: DataTypes.STRING(191),
      allowNull: true
    },
    vainglory_is_pro: {
      type: DataTypes.INTEGER(1),
      allowNull: true
    }
  }, {
    tableName: 'gamer',
    timestamps: false,
    underscored: true,
    freezeTableName: true
  });
};
