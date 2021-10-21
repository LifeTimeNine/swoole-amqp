<?php
/*
 * @Description   事件
 * @Author        lifetime
 * @Date          2021-07-19 19:14:41
 * @LastEditTime  2021-10-21 15:51:32
 * @LastEditors   lifetime
 */
namespace swooleamqp;

use swoole\event\TcpUdp;
use swoole\Output;
use Swoole\Timer;

/**
 * 事件类
 * @class Event
 */
class Event extends TcpUdp
{
    public static function onStart($server)
    {
        swoole_set_process_name('swoole_amqp');
    }
    
    public static function onWorkerStart($server, int $workerId)
    {
        if (!$server->taskworker) {
            $queueFile = require($server->config['queue_file_path']);
            // 初始化数量
            $initNum = 0;
            if (is_array($queueFile)) {
                // 异常队列列表
                $errorQueue = [];
                // 文件最后修改时间数组
                $fileTimes = [$server->config['queue_file_path'] => filectime($server->config['queue_file_path'] )];
                // 遍历队列列表
                foreach($queueFile as $class) {
                    // 如果类不存在，跳过
                    if (!class_exists($class)) continue;
                    /**
                     * 实例化
                     * @var \swooleamqp\Queue
                     */
                    $queue = $class::instance();
                    // 通过映射类获取文件名称
                    $filePath = (new \ReflectionClass($class))->getFileName();
                    // 记录文件最后修改时间
                    $fileTimes[$filePath] = filectime($filePath);
                    // 如果是禁用状态，跳过
                    if (!$queue->getEnable()) continue;
                    
                    // 创建定时器
                    $server->tick($queue->getWaitTime() * 1000, function() use($server, $class, $queue, &$errorQueue) {
                        // 如果在异常列表中
                        if (isset($errorQueue[$class])) {
                            // 如果在异常恢复时间之内，跳出
                            if (time() - $errorQueue[$class] < $server->config['error_recover_time']) {
                                return;
                            }
                        }
                        try {
                            // 创建订阅者
                            $consumer = $queue->createConsumer();
                            // 如果获取到新的消息
                            if ($message = $consumer->receiveNoWait()) {
                                $msg = unserialize($message->getBody());
                                // 异步任务执行逻辑
                                $server->task([
                                    'class' => $class,
                                    'msg' => $msg,
                                ], null, function($server, $taskId, $data) use($queue, $consumer, $message, $msg){
                                    // 如果执行成功，上报
                                    if ($data['result']) $consumer->acknowledge($message);
                                    // 记录日志
                                    self::recordLog($server, $queue, $msg, $data);
                                });
                            }
                            if (isset($errorQueue[$class])) {
                                unset($errorQueue[$class]);
                                self::output("[ {$class} ] restored");
                            }
                        } catch(\Exception $e) {
                            if (!isset($errorQueue[$class])) {
                                self::output("[ {$class} ] exception");
                            }
                            $errorQueue[$class] = time();
                            self::recordLog($server, $queue, null, null, $e);
                        }
                    });
                    $initNum ++;
                }
                // 如果开启了热更新
                if (!empty($server->config['hit_update'])) {
                    $server->tick(1000, function() use($server, $fileTimes){
                        if (count($fileTimes) == 1) clearstatcache();
                        foreach($fileTimes as $path => $lastUpdateTime) {
                            if ($lastUpdateTime <> filectime($path)) {
                                $server->reload();
                            }
                        }
                    });
                }
            }
            self::output("{$initNum} queues initialized");
        }
    }
    public static function onTask($server, int $task_id, int $src_worker_id, $data)
    {
        $queue = $data['class']::instance();
        $result = [
            'result' => true,
            'error' => false,
        ];
        try {
            $result['result'] = ($queue->execHandle($data['msg']) !== false);
        } catch(\Throwable $th) {
            $result['result'] = false;
            $result['error'] = true;
            $result['error_msg'] = $th->getMessage();
        }
        $server->finish($result);
    }

    public static function onWorkerExit($server, int $workerId)
    {
        Timer::clearAll();
    }

    /**
     * 记录日志
     * @param   \Swoole\Server      $server
     * @param   \swooleamqp\Queue   $queue
     * @param   array               $msg
     * @param   array               $result
     * @param   \Throwable          $th
     */
    protected static function recordLog($server, $queue, $msg = null, $result = null, $th = null)
    {
        if (!empty($path = $server->config['log_path'])) {
            $path = $path . date('Ym') . DIRECTORY_SEPARATOR . date('d') . DIRECTORY_SEPARATOR;
            if (!is_dir($path)) mkdir($path, 0777, true);
            $fileName = basename(str_replace('\\', '/', get_class($queue))) . '.log';
            $file = fopen($path . $fileName, 'a');
            flock($file, LOCK_EX);
            fwrite($file, str_pad('', 60, '-') . PHP_EOL);
            fwrite($file, "[ TIME ] " . date('Y-m-d H:i:s') . PHP_EOL);
            fwrite($file, "[ TOPIC ] " . $queue->getTopic() . PHP_EOL);
            fwrite($file, "[ QUEUE ] " . $queue->getQueue() . PHP_EOL);
            if (!empty($msg)) {
                fwrite($file, "[ MESSAGE ] " . var_export($msg, true) . PHP_EOL);
            }
            if (!empty($result)) {
                fwrite($file, "[ RESULT ] " . ($result['result'] ? 'TRUE' : 'FALSE') . PHP_EOL);
                if ($result['error']) {
                    fwrite($file, "[ ERROR ] " . $result['error_msg'] . PHP_EOL);
                }
            }
            if (!empty($th)) {
                fwrite($file, "[ ERROR ] " . $th->getMessage() . PHP_EOL);
            }
            flock($file, LOCK_UN);
            fclose($file);
        }
    }

    /**
     * 输出
     * @param   string  $msg
     * @param   boolean $showTime
     */
    protected static function output($msg, $showTime = true)
    {
        if ($showTime) $msg = '[' . date('Y-m-d H:i:s') . "] {$msg}";
        Output::instance()->writeln($msg);
    }
}
