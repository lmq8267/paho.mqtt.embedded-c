# MQTTClient-C 标准输出订阅器 stdoutsubc 的交叉编译脚本
#
# 本文件由 luci.mk 在构建目录 $(PKG_BUILD_DIR)/src 中调用:
#     $(MAKE) -C $(PKG_BUILD_DIR)/src clean compile
#     $(MAKE) -C $(PKG_BUILD_DIR)/src install      # DESTDIR=$(PKG_INSTALL_DIR)
#
# 交叉编译所需变量(CC / CFLAGS / LDFLAGS / STRIP / DESTDIR)均由 OpenWrt
# 构建系统通过命令行传入, 详见 include/package-defaults.mk。

# MQTT 源码根目录(即 paho.mqtt.embedded-c 目录), 由上级 Makefile 以环境变量传入
MQTT_SRC_DIR ?= $(error 未设置 MQTT_SRC_DIR, 无法定位 MQTT 源码目录)

STDOUTSUBC := stdoutsubc

# 头文件搜索路径
MQTT_INCLUDES := \
	-I$(MQTT_SRC_DIR)/MQTTClient-C/src \
	-I$(MQTT_SRC_DIR)/MQTTClient-C/src/linux \
	-I$(MQTT_SRC_DIR)/MQTTPacket/src

# 编译 stdoutsubc 所需的全部源码(MQTT 客户端 + 协议栈)
MQTT_SOURCES := \
	$(MQTT_SRC_DIR)/MQTTClient-C/samples/linux/stdoutsub.c \
	$(MQTT_SRC_DIR)/MQTTClient-C/src/MQTTClient.c \
	$(MQTT_SRC_DIR)/MQTTClient-C/src/linux/MQTTLinux.c \
	$(MQTT_SRC_DIR)/MQTTPacket/src/MQTTPacket.c \
	$(MQTT_SRC_DIR)/MQTTPacket/src/MQTTFormat.c \
	$(MQTT_SRC_DIR)/MQTTPacket/src/MQTTDeserializePublish.c \
	$(MQTT_SRC_DIR)/MQTTPacket/src/MQTTConnectClient.c \
	$(MQTT_SRC_DIR)/MQTTPacket/src/MQTTConnectServer.c \
	$(MQTT_SRC_DIR)/MQTTPacket/src/MQTTSubscribeClient.c \
	$(MQTT_SRC_DIR)/MQTTPacket/src/MQTTSubscribeServer.c \
	$(MQTT_SRC_DIR)/MQTTPacket/src/MQTTUnsubscribeClient.c \
	$(MQTT_SRC_DIR)/MQTTPacket/src/MQTTUnsubscribeServer.c \
	$(MQTT_SRC_DIR)/MQTTPacket/src/MQTTSerializePublish.c

# 最小体积相关编译/链接参数:
#   -Os                                   尺寸优先优化
#   -ffunction-sections / -fdata-sections 按函数与数据分节
#   -static                               全静态链接: 不依赖 libc.so / libgcc_s.so.1, 免装运行时库
#   -Wl,--gc-sections                     链接期回收未被引用的 section(剔除未用到的 server 端代码)
#   -lpthread                             多线程支持(musl 下由 libc 提供)
# 链接完成后再用交叉 strip 去除符号表, 进一步减小体积。
MIN_CFLAGS := -Os -ffunction-sections -fdata-sections -DMQTTCLIENT_PLATFORM_HEADER=MQTTLinux.h
MIN_LDFLAGS := -static -Wl,--gc-sections -lpthread

.PHONY: all compile clean install

all: compile

compile: $(STDOUTSUBC)

$(STDOUTSUBC): $(MQTT_SOURCES)
	$(CC) $(CFLAGS) $(MIN_CFLAGS) $(MQTT_INCLUDES) $(MQTT_SOURCES) $(LDFLAGS) $(MIN_LDFLAGS) -o $@
	$(STRIP) $@

clean:
	rm -f $(STDOUTSUBC)

install: compile
	mkdir -p $(DESTDIR)/usr/bin
	install -m0755 $(STDOUTSUBC) $(DESTDIR)/usr/bin/
