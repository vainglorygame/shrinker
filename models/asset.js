/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('asset', {
    id: {
      type: DataTypes.INTEGER(10).UNSIGNED,
      allowNull: false,
      primaryKey: true,
      autoIncrement: true
    },
    api_id: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    type: {
      type: DataTypes.STRING(191),
      allowNull: false
    }
  }, {
    tableName: 'asset',
    timestamps: false,
    underscored: true,
    freezeTableName: true
  });
};
