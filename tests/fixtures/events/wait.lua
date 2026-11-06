local ok, err = pcall(claviron.events.wait, "never")
assert(not ok and tostring(err):find("stopping"))
