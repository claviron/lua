local events = claviron.events
local done = events.listen({ "job.done", "job.failed" })
events.publish("job.done", { "for", "an", "application" }, { to = "php:octane-app" })
events.publish("elsewhere", {})
events.publish("job.done", {
    id = 7, big = math.maxinteger, ratio = 0.5, ok = true,
    steps = { "a", "b", "c" }, nested = { deep = { 1, 2 } },
    missing = claviron.json.null, raw = "\0\255",
})
local event = done:next()
assert(event.topic == "job.done" and #event.id > 0)
local p = event.payload
assert(p.id == 7 and p.big == math.maxinteger and p.ratio == 0.5 and p.ok == true)
assert(#p.steps == 3 and p.steps[1] == "a" and p.steps[3] == "c")
assert(p.nested.deep[2] == 2 and p.missing == claviron.json.null)
assert(p.raw == "\0\255", "binary strings travel")
assert(not pcall(events.publish, "bad", { f = print }))
assert(not pcall(events.listen, ""))
