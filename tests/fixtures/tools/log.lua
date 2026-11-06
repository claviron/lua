local ok, err = pcall(claviron.log.channel, "app")
assert(not ok and tostring(err):find("not declared"))
