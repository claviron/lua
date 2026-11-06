local served = 0
local router = claviron.router()

router:get("/users/{id}", function(request, params)
  served = served + 1
  return {
    status = 200,
    headers = { ["content-type"] = "application/json", ["set-cookie"] = { "a=1", "b=2" } },
    body = claviron.json.encode({
      id = params.id,
      served = served,
      query = request.query,
      seen = request.headers["x-seen"],
      body = request:body(),
    }),
  }
end)

router:post("/users", function() return "created" end)
router:get("/fail", function() error("broken") end)

return router
