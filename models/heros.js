/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('heros', {
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
    added_in_patch: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    name: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    api_name: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    is_assassin: {
      type: DataTypes.INTEGER(1),
      allowNull: false
    },
    is_mage: {
      type: DataTypes.INTEGER(1),
      allowNull: false
    },
    is_protector: {
      type: DataTypes.INTEGER(1),
      allowNull: false
    },
    is_sniper: {
      type: DataTypes.INTEGER(1),
      allowNull: false
    },
    is_warrior: {
      type: DataTypes.INTEGER(1),
      allowNull: false
    },
    is_carry: {
      type: DataTypes.INTEGER(1),
      allowNull: false
    },
    is_jungler: {
      type: DataTypes.INTEGER(1),
      allowNull: false
    },
    is_captain: {
      type: DataTypes.INTEGER(1),
      allowNull: false
    },
    heroic_perk: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    ability_a: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    ability_b: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    ability_c: {
      type: DataTypes.STRING(191),
      allowNull: false
    }
  }, {
    tableName: 'heros'
  });
};
