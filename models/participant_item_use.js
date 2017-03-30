/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('participant_item_use', {
    id: {
      type: DataTypes.INTEGER(10).UNSIGNED,
      allowNull: false,
      primaryKey: true,
      autoIncrement: true
    },
    participant_id: {
      type: DataTypes.STRING(191),
      allowNull: false,
      unique: true
    },
    item_id: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    action: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    timeFromStart: {
      type: DataTypes.INTEGER(11),
      allowNull: false
    }
  }, {
    tableName: 'participant_item_use'
  });
};
