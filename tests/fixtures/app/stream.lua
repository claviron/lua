local router = claviron.router()

-- The body in pieces, and a reply of one piece for each.
router:post("/echo", function(request)
  local pieces = {}
  while true do
    local piece = request:read()
    if piece == nil then break end
    pieces[#pieces + 1] = piece:upper()
  end
  local index = 0
  return {
    body = function()
      index = index + 1
      return pieces[index]
    end,
  }
end)

return router
