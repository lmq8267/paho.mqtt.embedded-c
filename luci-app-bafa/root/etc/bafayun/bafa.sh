#!/bin/sh

#主题名
title=$(echo $1 | tr ' \n')
#下发的指令内容
content=$(echo $2 | tr ' \n')
echo "[INFO] [$(date '+%Y-%m-%d %H:%M:%S')]：收到指令 ${title}  ${content}" >>/var/log/bafa.log

if [ -z "$content" ] ; then
	content=$(echo $1 | tr ' \n')
	title=$(uci -q get bafa.@bafa[0].topics)
fi
###############################################
ID="$(uci -q get bafa.@bafa[0].clientid | tr -d ' \n')"
#将指令推送到微信 方便查看路由是否收到 需要关注巴法云公众号，去掉下方前面的#启用 
send=$(curl -s "http://apis.bemfa.com/vb/wechat/v1/wechatWarn?uid=$ID&device=$title&message=$content")
if [ ! -z "$send" ] && [ "$(echo $send | grep -o 'success')" = "success" ] ; then
     echo "[INFO] [$(date '+%Y-%m-%d %H:%M:%S')]：微信推送成功！【${title}】${content}" >>/var/log/bafa.log
else
    echo "[INFO] [$(date '+%Y-%m-%d %H:%M:%S')]：微信推送失败！状态码：${send}" >>/var/log/bafa.log
fi
##############################################

############### 指令触发执行命令 #######################
# 说明：本脚本由 stdoutsubc 调用，参数如下：
#   $title   = 收到的主题名（例如：zerotier001）
#   $content = 收到的指令内容（例如：on / off）
# 可直接使用 if 条件同时判断主题和指令，在条件成立时执行相应命令并写入日志。
# 日志格式：echo "[级别] [$(date '+%Y-%m-%d %H:%M:%S')]：说明文字" >>/var/log/bafa.log
# 级别：INFO / WARN / ERROR

# 示例：如果主题是 zerotier001 且指令是 on，则执行开机相关命令
# if [ "$title" = "zerotier001" ] && [ "$content" = "on" ]; then
#     # 执行具体命令（示例）
#     /etc/init.d/zerotier start
#     echo "[INFO] [$(date '+%Y-%m-%d %H:%M:%S')]：主题【${title}】收到指令【${content}】，已启动 Zerotier" >>/var/log/bafa.log
# fi

# 示例：如果主题是 zerotier001 且指令是 off，则执行关机相关命令
# if [ "$title" = "zerotier001" ] && [ "$content" = "off" ]; then
#     /etc/init.d/zerotier stop
#     echo "[INFO] [$(date '+%Y-%m-%d %H:%M:%S')]：主题【${title}】收到指令【${content}】，已停止 Zerotier" >>/var/log/bafa.log
# fi

# 示例：同时支持多个主题的不同指令
# if [ "$title" = "bafa001" ] && [ "$content" = "reboot" ]; then
#     echo "[INFO] [$(date '+%Y-%m-%d %H:%M:%S')]：主题【${title}】收到指令【${content}】，设备即将重启" >>/var/log/bafa.log
#     reboot
# fi
# if [ "$title" = "bafa002" ] && [ "$content" = "update" ]; then
#     opkg update
#     echo "[INFO] [$(date '+%Y-%m-%d %H:%M:%S')]：主题【${title}】收到指令【${content}】，已执行更新" >>/var/log/bafa.log
# fi

# 示例：执行命令并记录输出结果
# if [ "$title" = "custom001" ] && [ "$content" = "status" ]; then
#     result=$(/etc/init.d/custom status 2>&1)
#     echo "[INFO] [$(date '+%Y-%m-%d %H:%M:%S')]：主题【${title}】指令【${content}】执行结果：${result}" >>/var/log/bafa.log
# fi
######################################################

