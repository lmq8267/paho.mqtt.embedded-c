module("luci.controller.bafa", package.seeall)

function index()
	if not nixio.fs.access("/etc/config/bafa") then
		return
	end

	local e=entry({"admin",  "services", "bafa"}, alias("admin", "services", "bafa", "setting"),_("巴法云物联网"), 46)
	e.dependent=false
	e.acl_depends={ "luci-app-bafa" }
	entry({"admin", "services", "bafa", "setting"}, cbi("bafa"), _("配置"), 47).leaf=true
	entry({"admin", "services", "bafa", "bafa_status"}, call("act_status"))
	entry({"admin", "services", "bafa", "bafa_log"}, form("bafa_log"), _("日志"), 48).leaf = true
	entry({"admin", "services", "bafa", "get_log"}, call("get_log"), nil).leaf = true
	entry({"admin", "services", "bafa", "clear_log"}, call("clear_log")).leaf = true
end

function act_status()
	local sys  = require "luci.sys"
	local e = { }
	e.running = sys.call("pidof stdoutsubc >/dev/null") == 0
	luci.http.prepare_content("application/json")
	luci.http.write_json(e)
end
function get_log()
    local log = ""
    local files = {"/var/log/bafa.log"}
    for i, file in ipairs(files) do
        if luci.sys.call("[ -f '" .. file .. "' ]") == 0 then
            log = log .. luci.sys.exec("cat " .. file)
        end
    end
    luci.http.write(log)
end

function clear_log()
	luci.sys.call("echo '' >/var/log/bafa.log")
end
