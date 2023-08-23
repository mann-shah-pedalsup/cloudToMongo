const mongoose = require("mongoose");

const stationSchema = mongoose.Schema({
  station_id: {
    type: String,
    required: true,
  },
  name: {
    type: String,
  },
  short_name: {
    type: String,
  },
  latitude: {
    type: Number,
  },
  longitude: {
    type: Number,
  },
  region_id: {
    type: Number,
  },
  rental_methods: {
    type: String,
  },
  capacity: {
    type: Number,
  },
  eightd_has_key_dispenser: {
    type: Boolean,
  },
  num_bikes_available: {
    type: Number,
  },
  num_bikes_disabled: {
    type: Number,
  },
  num_docks_available: {
    type: Number,
  },
  num_docks_disabled: {
    type: Number,
  },
  is_installed: {
    type: Boolean,
  },
  is_renting: {
    type: Boolean,
  },
  is_returning: {
    type: Boolean,
  },
  eightd_has_available_keys: {
    type: Boolean,
  },
  last_reported: {
    type: Date,
  },
});

const Stations = mongoose.model("Station", stationSchema);
module.exports = Stations;
