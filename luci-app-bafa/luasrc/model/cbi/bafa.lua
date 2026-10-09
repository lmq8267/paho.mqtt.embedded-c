local m, s ,o
local fs = require "nixio.fs"

m = Map("bafa")
m.title = translate("巴法云")
m.description = translate('通过MQTT协议连接巴法云，可接入米家等智能家居控制系统，实现远程控制语言控制设备。<br>【控制台】：<a href="https://cloud.bemfa.com/tcp/devicemqtt.html" target="_blank">cloud.bemfa.com/tcp/devicemqtt.html</a>')
m:section(SimpleSection).template = "bafa/bafa_status"

s = m:section(TypedSection, "bafa", translate("基本配置"))
s.addremove = false
s.anonymous = true

o = s:option(Flag,"enabled",translate("启用"))
o.default = 0

o = s:option(Value, "host",translate("服务器地址"))
o.default= "bemfa.com"

o = s:option(Value, "port",translate("服务器端口"))
o.datatype = "uinteger"
o.default= "9501"

o = s:option(Value, "topics",translate("主题名"),translate("多个主题请使用英文逗号分隔，必填项不能为空"))
o.placeholder = "test001,test002"

o = s:option(Value, "clientid",translate("账户私钥"),translate("必填项不能为空"))
o.password = true

o = s:option(ListValue, "qos", translate("QoS 等级"))
o.default= "1"
o:value("0",translate("0"))
o:value("1",translate("1"))
o:value("2",translate("2"))

o = s:option(Value, "username",translate("用户名"))

o = s:option(Value, "password",translate("密码"))
o.password = true

o = s:option(Flag, "showtopics", translate("消息显示主题名"))
o.default = 0

o = s:option(TextValue, "config", translate("触发脚本"))
o.rows = 20
o.wrap = "off"
o.cfgvalue = function(self, section)
    return nixio.fs.readfile("/etc/bafayun/bafa.sh") or ""
end
o.write = function(self, section, value)
    nixio.fs.writefile("/etc/bafayun/bafa.sh", value:gsub("\r\n", "\n"))
end

m.apply_on_parse = true
m.on_after_apply = function(self,map)
	luci.sys.exec("/etc/init.d/bafa restart")
end

return m

