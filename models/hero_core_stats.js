/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('hero_core_stats', {
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
    patch_version: {
      type: DataTypes.STRING(191),
      allowNull: false
    },
    hp: {
      type: "DOUBLE",
      allowNull: false
    },
    energy: {
      type: "DOUBLE",
      allowNull: false
    },
    weapon_damage: {
      type: "DOUBLE",
      allowNull: false
    },
    attack_speed: {
      type: "DOUBLE",
      allowNull: false
    },
    armor: {
      type: "DOUBLE",
      allowNull: false
    },
    shield: {
      type: "DOUBLE",
      allowNull: false
    },
    attack_range: {
      type: "DOUBLE",
      allowNull: false
    },
    move_speed: {
      type: "DOUBLE",
      allowNull: false
    },
    hp_regen: {
      type: "DOUBLE",
      allowNull: false
    },
    ep_regen: {
      type: "DOUBLE",
      allowNull: false
    }
  }, {
    tableName: 'hero_core_stats'
  });
};
