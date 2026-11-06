local cell = require "claviron.orbit.cell"
local n = cell.int(0)
local ok, err = pcall(n.wait, n, n:version())
assert(not ok and tostring(err):find("stopping"))
