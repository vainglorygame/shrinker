/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('keys', {
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
    type: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    key: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    value: {
      type: DataTypes.STRING(191),
      allowNull: false
    }
  }, {
    tableName: 'keys',
    timestamps: false,
    underscored: true,
    freezeTableName: true
  });
};
