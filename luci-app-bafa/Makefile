#
# Copyright (C) 2008-2014 The LuCI Team <luci@lists.subsignal.org>
#
# This is free software, licensed under the Apache License, Version 2.0 .
#
include $(TOPDIR)/rules.mk

PKG_NAME:=luci-app-bafa
PKG_VERSION:=1.1.0

PKG_SOURCE_PROTO:=git
PKG_SOURCE_URL:=https://github.com/lmq8267/paho.mqtt.embedded-c.git
PKG_SOURCE_VERSION:=master
PKG_SOURCE:=$(PKG_NAME)-$(PKG_VERSION)-paho.tar.gz
PKG_MIRROR_HASH:=skip
PKG_UNPACK=$(HOST_TAR) -xzf $(DL_DIR)/$(PKG_SOURCE) -C $(PKG_BUILD_DIR) --strip-components=1

LUCI_TITLE:=巴法云 (bemfa) LuCI 支持
LUCI_DEPENDS:=+luci-compat
LUCI_PKGARCH:=$(ARCH)

define Package/$(PKG_NAME)/description
    本软件包为巴法云 (bemfa) 提供 LuCI 配置页面。
    内置 MQTT 标准输出订阅器 stdoutsubc(安装于 /usr/bin/stdoutsubc)
endef

define Package/$(PKG_NAME)/postinst
#!/bin/sh
chmod +x /etc/init.d/bafa
endef

include $(TOPDIR)/feeds/luci/luci.mk

# 把解包后的 MQTT 源码根目录传给 src/Makefile(经环境变量)
export MQTT_SRC_DIR:=$(PKG_BUILD_DIR)

# 单独编译本包(make package/.../compile)时, 先自动下载源码
$(STAMP_PREPARED): $(DL_DIR)/$(PKG_SOURCE)

# call BuildPackage - OpenWrt buildroot signature
