/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('hero_stats', {
    id: {
      type: DataTypes.INTEGER(10).UNSIGNED,
      allowNull: false,
      primaryKey: true,
      autoIncrement: true
    },
    created_at: {
      type: DataTypes.TIME,
      allowNull: true
    },
    updated_at: {
      type: DataTypes.TIME,
      allowNull: true
    },
    pick_rate: {
      type: "DOUBLE(8,2)",
      allowNull: false
    },
    win_rate: {
      type: "DOUBLE(8,2)",
      allowNull: false
    },
    gold_per_min: {
      type: "DOUBLE(8,2)",
      allowNull: false
    },
    cs_per_min: {
      type: "DOUBLE(8,2)",
      allowNull: false
    }
  }, {
    tableName: 'hero_stats',
    timestamps: false,
    underscored: true,
    freezeTableName: true
  });
};
