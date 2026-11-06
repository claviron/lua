local greet = require "util.greet"
assert(greet.hi("lua") == "hi lua" and greet.name == "util.greet")
assert(require "util.greet" == greet and loads == 1, "loaded once")
assert((require "util").root)
assert(not pcall(require, "../etc/passwd"), "no path tricks")
assert(not pcall(require, "missing"))
