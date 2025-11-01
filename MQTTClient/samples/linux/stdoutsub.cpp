/*******************************************************************************
 * Copyright (c) 2012, 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution. 
 *
 * The Eclipse Public License is available at 
 *   http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at 
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Ian Craggs - initial contribution
 *    Ian Craggs - change delimiter option from char to string
 *******************************************************************************/

/*
 
 stdout subscriber
 
 compulsory parameters:
 
  topic to subscribe to
 
 defaulted parameters:
 
	--host localhost
	--port 1883
	--qos 2
	--delimiter \n
	--clientid stdout_subscriber
	
	--userid none
	--password none
 
*/
#include <stdarg.h>
#include <stdio.h>
#include <memory.h>
#include <time.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include "MQTTClient.h"
#include "linux.cpp"

volatile int toStop = 0;
#define MAX_CONCURRENT_SUBSCRIPTIONS 50

pid_t child_pids[MAX_CONCURRENT_SUBSCRIPTIONS];
int child_count = 0;

void log_message(const char *level, FILE *stream, const char *format, va_list args)
{
    time_t now;
    time(&now);

    now += 8 * 3600;
    struct tm *tm_info = gmtime(&now);

    char time_buf[20];
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", tm_info);

    fprintf(stream, "[%s] [%s]：", level, time_buf);
    vfprintf(stream, format, args);
    fprintf(stream, "\n");
	fflush(stream);
}

void log_info(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    log_message("INFO", stdout, format, args);
    va_end(args);
}

void log_error(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    log_message("ERROR", stderr, format, args);
    va_end(args);
}

void usage()
{
    printf("\n");
    printf("====================================\n");
    printf("       MQTT 标准输出订阅器         \n");
    printf("====================================\n");
    printf("用法: stdoutsub 主题名称 <命令>\n");
    printf("\n可选命令:\n");
    printf("  --host <主机名>         （默认: bemfa.com，MQTT 服务器地址）\n");
    printf("  --port <端口>           （默认: 9501，MQTT 服务器端口）\n");
    printf("  --qos <服务质量>        （默认: 1，MQTT QoS 等级，可选 0 或 1）\n");
    printf("  --delimiter <分隔符>    （默认: \\n，消息之间的分隔符）\n");
    printf("  --clientid <账户私钥>   （默认: 主机名 + 时间戳）\n");
    printf("  --username <用户名>     （默认: none，无用户名）\n");
    printf("  --password <密码>       （默认: none，无密码）\n");
    printf("  --showtopics <on|off>   （默认: off，是否显示主题名，若主题含通配符则默认开启）\n");
    printf("  --script <脚本路径>     （收到 MQTT 消息时，执行指定脚本）\n");
    printf("\n");
    printf("示例:\n");
    printf("  stdoutsub 主题名 --host bemfa.com --port 9501 --qos 1 --clientid asa48fd88e53d356ab21841a951284d\n");
    printf("\n");
    exit(-1);
}

/* ========== 信号处理函数 ========== */
void cfinish(int sig)
{
    signal(sig, SIG_IGN);
    toStop = 1;
    log_info("收到信号 %d，准备终止所有子进程...", sig);
    for (int i = 0; i < child_count; i++)
    {
        if (child_pids[i] > 0)
        {
            kill(child_pids[i], SIGTERM);
        }
    }
}

struct opts_struct
{
	char* clientid;
	int nodelimiter;
	char* delimiter;
	MQTT::QoS qos;
	char* username;
	char* password;
	char* host;
	int port;
	int showtopics;
	char* script;
} opts =
{
	(char*)"stdout-subscriber", 0, (char*)"\n", MQTT::QOS1, NULL, NULL, (char*)"bemfa.com", 9501, 0
};


void getopts(int argc, char** argv)
{
	int count = 2;
	
	while (count < argc)
	{
		if (strcmp(argv[count], "--qos") == 0)
		{
			if (++count < argc)
			{
				if (strcmp(argv[count], "0") == 0)
					opts.qos = MQTT::QOS0;
				else if (strcmp(argv[count], "1") == 0)
					opts.qos = MQTT::QOS1;
				else if (strcmp(argv[count], "2") == 0)
				{
					opts.qos = MQTT::QOS2;
					log_info("警告：MQTT 支持Qos0 Qos1等级，支持retian保留消息，不支持qos2，使用qos2会被强制下线，次数过多可造成账号异常无法使用。");
				}
				else
					usage();
			}
			else
				usage();
		}
		else if (strcmp(argv[count], "--host") == 0)
		{
			if (++count < argc)
				opts.host = argv[count];
			else
				usage();
		}
		else if (strcmp(argv[count], "--port") == 0)
		{
			if (++count < argc)
				opts.port = atoi(argv[count]);
			else
				usage();
		}
		else if (strcmp(argv[count], "--clientid") == 0)
		{
			if (++count < argc)
				opts.clientid = argv[count];
			else
				usage();
		}
		else if (strcmp(argv[count], "--username") == 0)
		{
			if (++count < argc)
				opts.username = argv[count];
			else
				usage();
		}
		else if (strcmp(argv[count], "--password") == 0)
		{
			if (++count < argc)
				opts.password = argv[count];
			else
				usage();
		}
		else if (strcmp(argv[count], "--delimiter") == 0)
		{
			if (++count < argc)
				opts.delimiter = argv[count];
			else
				opts.nodelimiter = 1;
		}
		else if (strcmp(argv[count], "--showtopics") == 0)
		{
			if (++count < argc)
			{
				if (strcmp(argv[count], "on") == 0)
				{
					opts.showtopics = 1;
					log_info("开启消息主题名显示。");
				}
				else if (strcmp(argv[count], "off") == 0)
					opts.showtopics = 0;
				else
					usage();
			}
			else
				usage();
		}
		else if (strcmp(argv[count], "--script") == 0)
       		{
            		if (++count < argc)
                		opts.script = argv[count];
            		else
                		usage();
        	}
		count++;
	}
	
}

int is_command_available(const char *command)
{
    char buffer[128];
    snprintf(buffer, sizeof(buffer), "which %s > /dev/null 2>&1", command);
    return system(buffer) == 0;  
}

void messageArrived(MQTT::MessageData& md)
{
	MQTT::Message &message = md.message;

	if (opts.showtopics)
		printf("%.*s\t", md.topicName.lenstring.len, md.topicName.lenstring.data);
	if (opts.nodelimiter)
		printf("%.*s", (int)message.payloadlen, (char*)message.payload);
	else
		printf("%.*s%s", (int)message.payloadlen, (char*)message.payload, opts.delimiter);
	fflush(stdout);
	if (opts.script)
    	{
        	char payload_buf[1024];
        	int len = (message.payloadlen < 1023) ? message.payloadlen : 1023;
        	memcpy(payload_buf, message.payload, len);
        	payload_buf[len] = '\0';

        	char command[2048];
        	const char *shell = NULL;

        	if (is_command_available("sh"))
        	{
            		shell = "sh";
        	}
        	else if (is_command_available("bash"))
        	{
            		shell = "bash";
        	}
        	else
        	{
            		log_error("错误：找不到可用的 shell（sh 或 bash）！");
            		return; 
        	}
        	if (opts.showtopics)
        	{
            		// 格式: <主题名> "<消息>"
            		snprintf(command, sizeof(command), "%s %s \"%.*s\" \"%s\" &",
                     	shell,
                     	opts.script,
                     	md.topicName.lenstring.len, md.topicName.lenstring.data,
                     	payload_buf);
        	}
        	else 
        	{
            		// 仅发送消息，不带主题 "<消息>"
            		snprintf(command, sizeof(command), "%s %s \"%s\" &", shell, opts.script, payload_buf);
        	}

        		system(command);
    	}
}

/* ========== MQTT 连接函数 ========== */
void myconnect(IPStack &ipstack, MQTT::Client<IPStack, Countdown, 1000, 50> &client, MQTTPacket_connectData &data, const char *topic)
{
	log_info("MQTT服务器 %s:%d ...", opts.host, opts.port);
    while (!toStop)
    {
        int rc = ipstack.connect(opts.host, opts.port);
        if (rc != 0)
        {
            log_error("【%s】TCP 连接失败，返回码：%d", topic, rc);
            sleep(5);
            continue;
        }

        rc = client.connect(data);
        if (rc != 0)
        {
            log_error("【%s】MQTT 连接失败，返回码：%d", topic, rc);
            ipstack.disconnect();
            sleep(5);
            continue;
        }

        log_info("【%s】连接成功！", topic);
        return;
    }
}

/* ========== 子进程逻辑 ========== */
void run_subscriber(const char *topic)
{
    signal(SIGINT, cfinish);
    signal(SIGTERM, cfinish);

    IPStack ipstack;
    MQTT::Client<IPStack, Countdown, 1000, 50> client(ipstack);

    MQTTPacket_connectData data = MQTTPacket_connectData_initializer;
    data.MQTTVersion = 3;
    data.clientID.cstring = opts.clientid;
    data.username.cstring = opts.username;
    data.password.cstring = opts.password;
    data.keepAliveInterval = 60;

    while (!toStop)
    {
        myconnect(ipstack, client, data, topic);
        if (!client.isConnected())
        {
            sleep(5);
            continue;
        }

        int rc = client.subscribe(topic, opts.qos, messageArrived);
        if (rc != 0)
        {
            log_error("【%s】订阅失败：%d", topic, rc);
            client.disconnect();
            ipstack.disconnect();
            sleep(5);
            continue;
        }

        log_info("【%s】成功订阅！", topic);

        while (!toStop)
        {
            rc = client.yield(1000);
            if (rc != 0 || !client.isConnected())
            {
                log_error("【%s】连接中断，重新连接中...", topic);
                break;
            }
        }

        client.disconnect();
        ipstack.disconnect();
        sleep(5);
    }

    log_info("【%s】进程退出。", topic);
    exit(0);
}

/* ========== 主进程逻辑 ========== */
int main(int argc, char **argv)
{
    if (argc < 2)
        usage();

    getopts(argc, argv);
    signal(SIGINT, cfinish);
    signal(SIGTERM, cfinish);

    char *topic_list = strdup(argv[1]);
    char *topic = strtok(topic_list, ",");

    while (topic != NULL && child_count < MAX_CONCURRENT_SUBSCRIPTIONS)
    {
        pid_t pid = fork();
        if (pid == 0)
        {
            run_subscriber(topic);
            exit(0);
        }
        else if (pid > 0)
        {
            log_info("主题【%s】创建子进程 pid=%d", topic, pid);
            child_pids[child_count++] = pid;
        }
        else
        {
            log_error("创建子进程失败（主题 %s）！", topic);
        }
        topic = strtok(NULL, ",");
    }

    /* 父进程等待所有子进程 */
    while (child_count > 0)
    {
        int status;
        pid_t pid = wait(&status);
        if (pid > 0)
        {
            log_info("子进程 %d 退出。", pid);
            child_count--;
        }
        else
            break;
    }

    log_info("所有子进程已退出，程序结束。");
    return 0;
}
