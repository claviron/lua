assert(os == nil)
assert(claviron.getenv("VALUE") == arg[1])
assert(claviron.getenv("EMPTY") == "")
assert(claviron.getenv("MISSING") == nil)
