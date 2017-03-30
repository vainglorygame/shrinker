/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('migrations', {
    id: {
      type: DataTypes.INTEGER(10).UNSIGNED,
      allowNull: false,
      primaryKey: true,
      autoIncrement: true
    },
    migration: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    batch: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    }
  }, {
    tableName: 'migrations'
  });
};
