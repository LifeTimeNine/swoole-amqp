<?php
/*
 * @Description   消费者服务
 * @Author        lifetime
 * @Date          2021-07-19 14:13:04
 * @LastEditTime  2021-10-21 15:33:01
 * @LastEditors   lifetime
 */
namespace swooleamqp;

use swoole\Config;
use swoole\server\TcpUdp;

/**
 * 消费者服务
 * @class Server
 */
class Server
{
    /**
     * 配置参数
     * @var array
     */
    protected $config = [
        'port' => 9503, // 端口
        'task_worker_num' => 1, // 任务工作进程数
        'pid_file' => '', // PID文件地址
        'queue_file_path' => '', // 队列列表文件地址
        'log_path' => '', // 日志文件地址
        'daemonize' => false, // 以守护进程的方式运行
        'hit_update' => true, // 热更新
        'error_recover_time' => 10, // 异常恢复时间 (0 表示关闭此功能)
    ];
    /**
     * 服务器实例
     * @var TcpUdp
     */
    protected $server;
    /**
     * 实例列表
     * @var array
     */
    protected static $instances = [];

    /**
     * 实例化
     * @param   array
     * @return  $this
     */
    public static function instance($config = [])
    {
        $key = md5(get_called_class() . serialize($config));
        if (isset(self::$instances[$key])) return self::$instances[$key];
        return self::$instances[$key] = new static($config);
    }

    /**
     * 构造函数
     * @param   array   $config
     */
    private function __construct($config = [])
    {
        if (is_array($config)) {
            $this->config = array_merge($this->config, $config);
        }
        if (!is_file($this->config['queue_file_path'])) {
            throw new \Exception('unable to access the task list file');
        }
        $swooleConfig = new Config();
        $swooleConfig->setPort($this->config['port']);
        $swooleConfig->setWorkerNum(1);
        $swooleConfig->setTaskWorkerNum($this->config['task_worker_num']);
        $swooleConfig->setPidFile($this->config['pid_file']);
        $swooleConfig->setEventClass('\swooleamqp\Event');
        $swooleConfig->setMaxWaitTime(10);
        $swooleConfig->setReloadAsync(true);
        $swooleConfig->setDaemonize($this->config['daemonize'] == true);
        if (!empty($this->config['log_path'])) {
            $swooleConfig->setLogFile($this->config['log_path'] . "/swoole_amqp.log");
        }
        $this->server = TcpUdp::instance($swooleConfig)->setName('SwooleAmqp');
    }

    /**
     * 设置参数
     * @param   array   $config
     * @return  $this
     */
    public function setConfig($cofig)
    {
        if (is_array($cofig)) {
            $this->config = array_merge($this->config, $cofig);
        }
        if (!is_file($this->config['queue_file_path'])) {
            throw new \Exception('unable to access the task list file');
        }
        return $this;
    }
    /**
     * 启动服务器
     */
    public function start()
    {
        $this->server->initServer();
        $this->server->getServer()->config = $this->config;
        $this->server->start();
    }
    /**
     * 停止服务器
     */
    public function stop()
    {
        $this->server->stop();
    }
    /**
     * 柔性重启服务器
     */
    public function reload()
    {
        $this->server->reload();
    }
    /**
     * 重启服务器
     */
    public function restart()
    {
        $this->server->restart(function($server) {
            $server->config = $this->config;
        });
    }
    /**
     * 删除PID文件
     */
    public function removePidFile()
    {
        $this->server->removePidFile();
    }
}
