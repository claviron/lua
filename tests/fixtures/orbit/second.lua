local cell = require "claviron.orbit.cell"
local n = cell.open(arg[1])
assert(n:get() == math.maxinteger and n:kind() == "int")
