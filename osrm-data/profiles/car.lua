# From the project root (c:\Users\aryan\OneDrive\Desktop\MediGrid-)

# Create the folder (if it doesn't already exist)
New-Item -ItemType Directory -Force -Path .\osrm-data\profiles

# Write a minimal car profile (no external Lua libs)
@"
api_version = 4

Set = require('lib/set')
Sequence = require('lib/sequence')
Utils = require('lib/utils')

function setup()
  return {
    properties = {
      max_speed_for_map_matching = 180/3.6,
      weight_name = 'duration',
      use_turn_restrictions = false,   -- ignore turn restrictions
    },
    default_mode = mode.driving,
    default_speed = 50,               -- km/h if tag missing
    highway_speeds = {
      motorway      = 90,
      motorway_link = 60,
      trunk         = 85,
      trunk_link    = 50,
      primary       = 65,
      primary_link  = 40,
      secondary     = 50,
      secondary_link= 30,
      tertiary      = 40,
      residential   = 30,
      living_street = 15,
      unclassified  = 30,
      service       = 20,
    }
  }
end

function process_node(profile, node, result)
  -- intentionally empty – we ignore barriers/obstacles
end

function process_way(profile, way, result)
  local highway = way:get_value_by_key('highway')
  if not highway then return end
  local speed = profile.highway_speeds[highway]
  if not speed then return end
  result.flag:set_value(profile.default_mode, true)
  result.forward_speed = speed
  result.backward_speed = speed
  local function set_weights(s) 
    result.forward_weight = way:length() / (s/3.6)
    result.backward_weight = way:length() / (s/3.6)
  end
  set_weights(speed)
end

function process_turn(profile, turn)
  -- no turn penalties
  turn.duration = 0
  turn.weight = 0
end

return {
  setup = setup,
  process_node = process_node,
  process_way = process_way,
  process_turn = process_turn,
}
"@ | Set-Content -Encoding UTF8 -Path .\osrm-data\profiles\car.lua