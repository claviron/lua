local json = claviron.json
local value = json.decode('{"id":7,"tags":[],"note":null,"big":9007199254740993}')
assert(value.id == 7 and value.note == json.null and #value.tags == 0)
assert(value.big == 9007199254740993, "a 64-bit integer survives")
local back = json.decode(json.encode(value))
assert(json.encode(back.tags) == "[]" and back.note == json.null)
assert(json.encode(json.array({})) == "[]" and json.encode({}) == "{}")
assert(json.encode({1, 2, 3}) == "[1,2,3]")
assert(not pcall(json.decode, "{bad"))
