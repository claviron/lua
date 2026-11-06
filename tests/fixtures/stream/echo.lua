local peer = claviron.stream.open(arg[1])
assert(not pcall(peer.ticket, peer), "the opener holds no ticket")
while true do
    local message = peer:receive()
    if message == nil then break end
    peer:send(message:upper())
end
peer:finish()
