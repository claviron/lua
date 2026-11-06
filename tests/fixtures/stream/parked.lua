local s = claviron.stream()
local ok, error = pcall(s.read, s)
assert(not ok and tostring(error):find("stopping"), tostring(error))
