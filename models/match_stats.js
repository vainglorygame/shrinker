/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('match_stats', {
    id: {
      type: DataTypes.INTEGER(10).UNSIGNED,
      allowNull: false,
      primaryKey: true,
      autoIncrement: true
    }
  }, {
    tableName: 'match_stats'
  });
};
