<div align="center">
  <img src="doc/pahologo.png" alt="Eclipse Paho" width="320">
</div>

<h1 align="center">Eclipse Paho MQTT C/C++ 嵌入式平台客户端</h1>

本仓库包含 [Eclipse Paho](http://eclipse.org/paho) 的 MQTT C/C++ 客户端库源代码，面向嵌入式平台。

代码采用 EPL 与 EDL 双重许可（详见 about.html 与 notice.html）。你可以自行选择使用其中一种许可。EDL 允许你将本代码嵌入自有应用，并以二进制或源码形式分发，而无需向 Paho 回馈你的任何代码或改动。具体条件请参阅 EDL 的完整条款。

本仓库包含三个子项目：

1. **MQTTPacket** —— MQTT 报文的简单序列化/反序列化，以及若干辅助函数
2. **MQTTClient** —— 较高级别的 C++ 客户端
3. **MQTTClient-C** —— 较高级别的 C 客户端（基本上是 C++ 客户端的移植版本）

*MQTTPacket* 目录包含最低层的 C 库，依赖要求最小，提供简单的序列化与反序列化例程。它既是上层库的基础，也可以单独使用；报文的网络收发主要由使用方自行实现。

*MQTTClient* 目录包含高一级的 C++ 库。网络相关代码封装在独立的类中，便于接入你所需的网络实现。目前提供 Linux、Arduino 与 mbed 平台的实现。ARM mbed 最初是为其编写该库的平台，而该平台的常规语言选择是 C++，这也解释了为何采用 C++ 编写。作者另撰写了一篇入门级的[移植指南](http://modelbasedtesting.co.uk/2014/08/25/porting-a-paho-embedded-c-client/)。

*MQTTClient-C* 目录包含与 MQTTClient 对应的 C 版本，面向不支持 C++ 或不以 C++ 为惯例的平台。在可行范围内，它是 *MQTTClient* 的直接翻译。

## 构建要求 / 编译

各模块已引入 CMake 构建，并配置了 Travis-CI 用于自动构建与测试。在 Linux 上，基本构建方式如下：

```
mkdir build.paho
cd build.paho
cmake ..
make
```

`travis-build.sh` 文件包含了 Linux 下完整的构建与测试流程。

## 用法与 API

请参阅各 samples 目录中的示例用法。各模块的 Doxygen 配置文件位于 doc 目录中。

## 运行时跟踪

*MQTTClient* API 内置了对收发的 MQTT 报文的调试跟踪功能——通过定义 `MQTT_DEBUG` 预处理宏即可开启。

## LuCI 应用（luci-app-bafa）

本仓库同时附带一个 OpenWrt LuCI 应用 **luci-app-bafa**，用于在 OpenWrt 设备上接入 **巴法云（bemfa）** 的 MQTT 服务。

### 简介

- **目录位置**：`luci-app-bafa/`
- **功能**：提供 LuCI 配置页面，并内置命令行工具 **`stdoutsubc`**，用于订阅 MQTT 主题并将收到的消息输出到标准输出，或交由指定脚本处理。
- **编译方式**：编译时会**自动从本仓库拉取最新 MQTT 源码**（`MQTTClient-C` + `MQTTPacket`），将其**静态合并**编译为单个可执行文件 `stdoutsubc`，安装到 `/usr/bin/stdoutsubc`。该文件**完全静态链接**，不依赖额外的 `.so` 动态库。

### 集成到固件

`luci-app-bafa` 已做到自包含，集成固件时**只需该目录即可**（无需额外准备源码目录）：

1. 将 `luci-app-bafa` 目录复制到 OpenWrt SDK（或源码树）的 `package/` 下：
   ```
   cp -a luci-app-bafa <SDK>/package/
   ```
2. 在 `menuconfig` 中选中 `luci-app-bafa`（即 `CONFIG_PACKAGE_luci-app-bafa=y`）
3. 编译：
   ```
   make package/luci-app-bafa/compile
   ```

编译过程中，包内 `PKG_SOURCE` 会自动从 `https://github.com/lmq8267/paho.mqtt.embedded-c` 拉取 MQTT 源码并完成 `stdoutsubc` 的交叉编译。仓库内亦提供 GitHub Actions 工作流（`.github/workflows/luci.yml`），可一键为多种架构（22.03 的 `ipk` 与 snapshot 的 `apk`）打包。

### stdoutsubc 用法

```
stdoutsubc 主题名称 <命令>
```

支持的命令：

| 参数 | 说明 | 默认值 |
| --- | --- | --- |
| `--host <主机名>` | MQTT 服务器地址 | `bemfa.com` |
| `--port <端口>` | MQTT 服务器端口 | `9501` |
| `--qos <服务质量>` | MQTT QoS 等级，可选 `0` 或 `1` | `1` |
| `--delimiter <分隔符>` | 消息之间的分隔符 | `\n` |
| `--clientid <账户私钥>` | 客户端 ID（巴法云的账户私钥） | 主机名 + 时间戳 |
| `--username <用户名>` | 用户名 | 无 |
| `--password <密码>` | 密码 | 无 |
| `--showtopics <on\|off>` | 是否显示主题名（主题含通配符 `#`/`+` 或订阅多个主题时自动开启） | `off` |
| `--script <脚本路径>` | 收到 MQTT 消息时执行指定脚本 | 无 |
| `-h, --help` | 显示帮助信息 | — |

示例（含消息处理脚本）：

```
stdoutsubc 主题名 --host bemfa.com --port 9501 --qos 1 --clientid asa48fd88e53d356ab21841a951284d --script /etc/bafayun/on_message.sh
```

### 脚本示例

`--script` 指定的脚本由 `sh` 调用，收到消息时在后台执行。传入参数的形式与 `--showtopics` 有关：

- 开启 `--showtopics` 时：`<脚本> "<主题名>" "<消息内容>"`
- 未开启时：`<脚本> "<消息内容>"`

保存为 `/etc/bafayun/on_message.sh` 并赋予执行权限（`chmod +x /etc/bafayun/on_message.sh`）：

```sh
#!/bin/sh
# 用法:
#   开启 --showtopics 时: on_message.sh "<主题名>" "<消息内容>"
#   未开启时:             on_message.sh "<消息内容>"

if [ $# -ge 2 ]; then
    topic="$1"
    msg="$2"
else
    topic=""
    msg="$1"
fi

# 示例: 记录到日志文件
echo "$(date '+%Y-%m-%d %H:%M:%S') [$topic] $msg" >> /tmp/bafa-messages.log

# 示例: 根据消息内容执行动作
case "$msg" in
    on|open)
        # 打开继电器 / 执行你的控制命令
        ;;
    off|close)
        # 关闭继电器 / 执行你的控制命令
        ;;
esac
```

## 报告问题

本项目通过 GitHub Issues 跟踪开发进展与问题：[github.com/eclipse/paho.mqtt.embedded-c/issues](https://github.com/eclipse/paho.mqtt.embedded-c/issues)。

## 更多信息

关于 Paho 客户端的讨论位于 [Eclipse Mattermost Paho 频道](https://mattermost.eclipse.org/eclipse/channels/paho) 与 [Eclipse paho-dev 邮件列表](https://dev.eclipse.org/mailman/listinfo/paho-dev)。

有关 MQTT 协议的常规问题，可在 [MQTT Google Group](https://groups.google.com/forum/?hl=en-US&fromgroups#!forum/mqtt) 讨论。

更多信息请访问 [MQTT 社区](http://mqtt.org)。
