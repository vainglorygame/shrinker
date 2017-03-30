/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('hero_dimension', {
    id: {
      type: DataTypes.INTEGER(10).UNSIGNED,
      allowNull: false,
      primaryKey: true,
      autoIncrement: true
    },
    hero_id: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    dimension_id: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    stats_id: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    },
    computed_on: {
      type: DataTypes.DATE,
      allowNull: false
    }
  }, {
    tableName: 'hero_dimension'
  });
};