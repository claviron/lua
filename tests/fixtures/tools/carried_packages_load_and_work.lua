local inspect = require "inspect"
assert(inspect({ a = 1 }) == "{\n  a = 1\n}")

local lume = require "lume"
assert(lume.reduce(lume.map({ 1, 2, 3 }, function(x) return x * 2 end), function(a, b) return a + b end) == 12)
assert(#lume.uuid() == 36)
assert(not pcall(lume.time, function() end), "lume.time needs os")

local class = require "middleclass"
local Animal = class("Animal")
function Animal:initialize(name) self.name = name end
function Animal:speak() return self.name .. " makes a sound" end
local Dog = class("Dog", Animal)
function Dog:speak() return self.name .. " barks" end
assert(Dog:new("Rex"):speak() == "Rex barks" and Dog:new("Rex"):isInstanceOf(Animal))

local serpent = require "serpent"
local ok, copy = serpent.load(serpent.line({ 1, nested = { x = true } }))
assert(ok and copy.nested.x == true and copy[1] == 1)
