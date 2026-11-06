local invoke = claviron.invoke
local resize = invoke.subscribe("image.resize")
invoke.send("other.op", {})
local id = invoke.send("image.resize", { path = "a.png", sizes = { 320, 640 } })
local call = resize:next()
assert(call.id == id and call.operation == "image.resize")
assert(call.payload.path == "a.png" and call.payload.sizes[2] == 640)
assert(call.submitted_at > 0)
assert(not pcall(invoke.send, "", {}))
