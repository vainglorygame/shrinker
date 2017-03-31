/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('participant_ext', {
    id: {
      type: DataTypes.INTEGER(10).UNSIGNED,
      allowNull: false,
      primaryKey: true,
      autoIncrement: true
    },
    series: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    participant_api_id: {
      type: DataTypes.STRING(191),
      allowNull: false,
      unique: true
    },
    role: {
      type: DataTypes.STRING(191),
      allowNull: true
    },
    score: {
      type: "DOUBLE(8,2)",
      allowNull: true
    },
    kda: {
      type: "DOUBLE(8,2)",
      allowNull: true
    },
    kills_participation: {
      type: "DOUBLE(8,2)",
      allowNull: true
    }
  }, {
    tableName: 'participant_ext',
    timestamps: false,
    underscored: true,
    freezeTableName: true
  });
};
