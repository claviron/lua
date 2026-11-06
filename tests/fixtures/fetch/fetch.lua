local base = arg[1]
local r = claviron.fetch(base .. "/ok", { method = "post", body = "hi" })
assert(r.ok and r.status == 200 and r.body == "POST hi", r.body)
assert(r.headers["x-seen"] == "one, two")
local ok, err = pcall(claviron.fetch, "http://example.com/")
assert(not ok and tostring(err):find("not a host fetch allows"))
ok, err = pcall(claviron.fetch, base .. "/away")
assert(not ok and tostring(err):find("redirect"), tostring(err))
assert(not pcall(claviron.fetch, "file:///etc/passwd"))
