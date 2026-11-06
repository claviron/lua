local date = claviron.regex("(?<year>\\d{4})-(\\d{2})")
assert(date:is_match("x 2026-09 y"))
local s, e = date:find("on 2026-09")
assert(s == 4 and e == 10)
local m = date:match("2026-09")
assert(m[0] == "2026-09" and m[2] == "09" and m.year == "2026")
local seen = {}
for found in date:gmatch("2026-09 2027-10") do seen[#seen + 1] = found.year end
assert(#seen == 2 and seen[2] == "2027")
assert(date:replace("2026-09", "$2/$year") == "09/2026")
local parts = claviron.regex(",\\s*"):split("a, b,c")
assert(#parts == 3 and parts[3] == "c")
assert(not pcall(claviron.regex, "("))
