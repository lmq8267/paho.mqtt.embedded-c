/*******************************************************************************
 * Copyright (c) 2012, 2016 IBM Corp.
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
 *    Al Stockdill-Mander - Version using the embedded C client
 *    Ian Craggs - update MQTTClient function names
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

 for example:

    stdoutsub topic/of/interest --host iot.eclipse.org

*/
#include <stdarg.h>
#include <stdio.h>
#include <memory.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/prctl.h>  // prctl 用于父进程死亡时对子进程发送信号
#include "MQTTClient.h"

volatile int toStop = 0;

#define MAX_CONCURRENT_SUBSCRIPTIONS 50

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

struct opts_struct
{
    char* clientid;
    int nodelimiter;
    char* delimiter;
    enum QoS qos;
    char* username;
    char* password;
    char* host;
    int port;
    int showtopics;
    char* script;
} opts =
{
    (char*)"stdout-subscriber", 0, (char*)"\n", QOS1, NULL, NULL, (char*)"bemfa.com", 9501, 0
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
                {
                    opts.qos = QOS0;
                }
                else if (strcmp(argv[count], "1") == 0)
                {
                    opts.qos = QOS1;
                }
                else if (strcmp(argv[count], "2") == 0)
                {
                    opts.qos = QOS2;
                    log_info("警告：MQTT 支持Qos0 Qos1等级，支持retian保留消息，不支持qos2，使用qos2会被强制下线，次数过多可造成账号异常无法使用。");
                }
                else
                {
                    usage();
                }
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
                {
                    opts.showtopics = 0;
                }
                else
                {
                    usage();
                }
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

void messageArrived(MessageData* md)
{
    MQTTMessage* message = md->message;

    if (opts.showtopics)
        printf("%.*s\t", md->topicName->lenstring.len, md->topicName->lenstring.data);
    if (opts.nodelimiter)
        printf("%.*s", (int)message->payloadlen, (char*)message->payload);
    else
        printf("%.*s%s", (int)message->payloadlen, (char*)message->payload, opts.delimiter);

    if (opts.script)
    {
        char payload_buf[1024];
        int len = (message->payloadlen < 1023) ? message->payloadlen : 1023;
        memcpy(payload_buf, message->payload, len);
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
                     md->topicName->lenstring.len, md->topicName->lenstring.data,
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

// 子进程收到终止信号后的处理：将 toStop 置为 1，退出循环并优雅断开
void child_signal_handler(int sig)
{
    toStop = 1;
}

// 子进程：订阅并持续重连（与原来线程循环类似，但作为独立进程运行）
void subscribeTopic_process(const char* topic)
{
    int rc = 0;
    MQTTClient c;
    unsigned char buf[100];
    unsigned char readbuf[100];
    Network n;

    // 当父进程死亡时，也希望子进程能优雅结束：设置在父进程死亡时内核给子进程发送 SIGHUP
    prctl(PR_SET_PDEATHSIG, SIGHUP);

    // 子进程捕捉 SIGINT SIGTERM SIGHUP 以便优雅退出
    struct sigaction sa;
    sa.sa_handler = child_signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGHUP, &sa, NULL);

	
    log_info("MQTT服务器 %s:%d", opts.host, opts.port);
    while (!toStop)
    {
        NetworkInit(&n);
        rc = NetworkConnect(&n, opts.host, opts.port);
        if (rc != 0)
        {
            log_error("【%s】无法连接服务器，状态码：%d，5 秒后重试...", topic, rc);
            NetworkDisconnect(&n);
            sleep(5);
            continue;
        }

        MQTTClientInit(&c, &n, 1000, buf, sizeof(buf), readbuf, sizeof(readbuf));
        MQTTPacket_connectData data = MQTTPacket_connectData_initializer;
        data.willFlag = 0;
        data.MQTTVersion = 3;
        data.clientID.cstring = opts.clientid;
        data.username.cstring = opts.username;
        data.password.cstring = opts.password;
        data.keepAliveInterval = 60;
        data.cleansession = 0;

        rc = MQTTConnect(&c, &data);
        if (rc != 0)
        {
            log_error("【%s】MQTT 连接失败，状态码：%d，5 秒后重试...", topic, rc);
            NetworkDisconnect(&n);
            sleep(5);
            continue;
        }

        log_info("【%s】成功连接到服务器！", topic);

        rc = MQTTSubscribe(&c, topic, opts.qos, messageArrived);
        if (rc != 0)
        {
            log_error("【%s】订阅失败，状态码：%d，5 秒后重试...", topic, rc);
            MQTTDisconnect(&c);
            NetworkDisconnect(&n);
            sleep(5);
            continue;
        }

        log_info("【%s】成功订阅主题！", topic);

        // 主循环：yield 接收消息，若返回非 0 则说明连接中断，跳出重连
        while (!toStop)
        {
            rc = MQTTYield(&c, 1000);
            if (rc != 0)
            {
                log_error("【%s】MQTTYield 返回错误，状态码：%d，准备重连...", topic, rc);
                break;
            }
        }

        log_info("【%s】断开订阅或收到退出信号，正在清理连接...", topic);
        MQTTDisconnect(&c);
        NetworkDisconnect(&n);

        if (!toStop)
        {
            // 非主动退出，等待一会儿再重连（简单退避）
            sleep(5);
        }
    }

    log_info("【%s】子进程退出，释放资源。", topic);
    // 子进程优雅退出
    _exit(0);
}

// 父进程需要的全局状态
volatile sig_atomic_t parent_shutdown = 0;
pid_t children_pids[MAX_CONCURRENT_SUBSCRIPTIONS];
char* topics_for_children[MAX_CONCURRENT_SUBSCRIPTIONS];
int topic_count = 0;

// 父进程信号处理：设置标志并向所有子进程发送 SIGTERM
void parent_signal_handler(int sig)
{
    parent_shutdown = 1;
    // 发送 SIGTERM 给所有已知子进程，通知它们退出
    for (int i = 0; i < topic_count; i++)
    {
        if (children_pids[i] > 0)
        {
            kill(children_pids[i], SIGTERM);
        }
    }
}

// 根据主题索引创建子进程并记录 pid
pid_t spawn_child_for_index(int idx)
{
    pid_t pid = fork();
    if (pid == 0)
    {
        // 子进程执行订阅逻辑
        subscribeTopic_process(topics_for_children[idx]);
        // 不应该返回到这里，但若返回，强制退出
        _exit(0);
    }
    else if (pid > 0)
    {
        children_pids[idx] = pid;
        log_info("主题【%s】创建子进程 pid=%d", topics_for_children[idx], pid);
    }
    else
    {
        log_error("创建子进程失败（主题 %s），errno=%d (%s)", topics_for_children[idx], errno, strerror(errno));
        children_pids[idx] = -1;
    }
    return pid;
}

// 主函数：解析主题，fork 子进程并监护
int main(int argc, char** argv)
{
    if (argc < 2) usage();

    char* topic_list = argv[1];
    getopts(argc, argv);

    // 先把主题拆分并保存到数组，方便后续重启使用
    char* copy = strdup(topic_list);
    if (!copy) { log_error("内存分配失败"); return -1; }

    char* tok = strtok(copy, ",");
    topic_count = 0;
    while (tok && topic_count < MAX_CONCURRENT_SUBSCRIPTIONS)
    {
        // 保存 strdup 的副本以便父进程重复使用
        topics_for_children[topic_count] = strdup(tok);
        // 若主题包含通配符或多个主题则默认显示主题名
        if (strchr(tok, '#') || strchr(tok, '+')) opts.showtopics = 1;
        topic_count++;
        tok = strtok(NULL, ",");
    }
    free(copy);

    if (topic_count == 0)
    {
        log_error("没有找到任何主题，退出。");
        return -1;
    }
    if (topic_count > 1) opts.showtopics = 1;

    // 初始化 children_pids
    for (int i = 0; i < topic_count; i++) children_pids[i] = -1;

    // 父进程捕捉 SIGINT/SIGTERM，用于优雅关闭所有子进程
    struct sigaction psa;
    psa.sa_handler = parent_signal_handler;
    sigemptyset(&psa.sa_mask);
    psa.sa_flags = 0;
    sigaction(SIGINT, &psa, NULL);
    sigaction(SIGTERM, &psa, NULL);

    // 为每个主题 fork 一个子进程
    for (int i = 0; i < topic_count; i++)
    {
        spawn_child_for_index(i);
        // 小延迟，避免瞬时重启风暴
        usleep(100000);
    }

    // 父进程监护循环：当某个子进程异常退出且父未触发关闭时，重启该子进程
    while (!parent_shutdown)
    {
        int status;
        pid_t dead = waitpid(-1, &status, WNOHANG);
        if (dead == 0)
        {
            // 没有子进程退出，稍作休眠后继续监控
            sleep(1);
            continue;
        }
        else if (dead > 0)
        {
            // 找到对应的索引
            int idx = -1;
            for (int i = 0; i < topic_count; i++)
            {
                if (children_pids[i] == dead)
                {
                    idx = i;
                    break;
                }
            }
            if (idx >= 0)
            {
                if (WIFEXITED(status))
                    log_info("子进程 pid=%d（主题 %s）正常退出，退出码=%d", dead, topics_for_children[idx], WEXITSTATUS(status));
                else if (WIFSIGNALED(status))
                    log_error("子进程 pid=%d（主题 %s）被信号 %d 终止", dead, topics_for_children[idx], WTERMSIG(status));
                else
                    log_error("子进程 pid=%d（主题 %s）退出，状态未知", dead, topics_for_children[idx]);

                children_pids[idx] = -1;

                // 如果父进程并未准备关闭，则自动重启该子进程（带退避）
                if (!parent_shutdown)
                {
                    log_info("准备重启主题【%s】对应的子进程...", topics_for_children[idx]);
                    sleep(1); // 简单退避
                    spawn_child_for_index(idx);
                }
            }
            else
            {
                // 未知子进程退出，记录日志
                log_error("检测到未知子进程 pid=%d 退出。", dead);
            }
        }
        else
        {
            // waitpid 返回错误
            if (errno == ECHILD)
            {
                // 没有子进程了，退出监护循环
                break;
            }
            else
            {
                log_error("waitpid 错误，errno=%d (%s)", errno, strerror(errno));
                sleep(1);
            }
        }
    }

    // 父进程开始优雅关闭流程：确保所有子进程收到终止信号并退出
    log_info("父进程收到退出信号，开始终止所有子进程...");
    for (int i = 0; i < topic_count; i++)
    {
        if (children_pids[i] > 0)
        {
            // 若子进程仍在，先发送 SIGTERM，等待短暂时间，若仍未退出再发送 SIGKILL
            kill(children_pids[i], SIGTERM);
        }
    }

    // 等待一段时间让子进程自行退出
    const int max_wait_seconds = 10;
    time_t start = time(NULL);
    while (1)
    {
        int all_gone = 1;
        for (int i = 0; i < topic_count; i++)
        {
            if (children_pids[i] > 0)
            {
                pid_t w = waitpid(children_pids[i], NULL, WNOHANG);
                if (w == 0)
                {
                    all_gone = 0;
                }
                else
                {
                    children_pids[i] = -1;
                }
            }
        }
        if (all_gone) break;
        if (time(NULL) - start >= max_wait_seconds) break;
        usleep(200000);
    }

    // 对仍然存在的子进程发送 SIGKILL 强制结束
    for (int i = 0; i < topic_count; i++)
    {
        if (children_pids[i] > 0)
        {
            log_error("子进程 pid=%d 未能优雅退出，发送 SIGKILL 强制结束。", children_pids[i]);
            kill(children_pids[i], SIGKILL);
            waitpid(children_pids[i], NULL, 0);
            children_pids[i] = -1;
        }
    }

    // 释放父进程保存的主题字符串
    for (int i = 0; i < topic_count; i++)
    {
        if (topics_for_children[i]) free(topics_for_children[i]);
    }

    log_info("所有子进程已终止，父进程退出。");
    return 0;
}
