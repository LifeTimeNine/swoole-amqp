<?php
/*
 * @Description   事件
 * @Author        lifetime
 * @Date          2021-07-19 19:14:41
 * @LastEditTime  2021-09-19 08:50:16
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
            if (is_array($queueFile) && count($queueFile) > 0) {
                $queueList = [];
                foreach($queueFile as $item) {
                    if (!class_exists($item)) continue;
                    try {
                        $queue = $item::instance();
                        if (!$queue->getEnable()) continue;
                        $queueList[] = [
                            'class' => $item,
                            'queue' => $queue,
                            'consumer' => $queue->createConsumer(),
                        ];
                    } catch(\Exception $e) {
                        self::errorRecover($server, $item, $queueList);
                        self::writeError($server, $item, $e);
                    }
                }
                $server->tick(100, function() use($server, &$queueList){
                    $newQueueList = [];
                    foreach($queueList as $queue) {
                        try {
                            if ($message = $queue['consumer']->receiveNoWait()) {
                                $server->task([
                                    'class' => $queue['class'],
                                    'msg' => unserialize($message->getBody()),
                                ], null, function($server, $task_id, $data) use($queue, $message){
                                    if ($data['result'] !== false) $queue['consumer']->acknowledge($message);
                                    self::writeLog($server, $data);
                                });
                            }
                            $newQueueList[]  = $queue;
                        } catch(\Exception $e) {
                            self::errorRecover($server, $queue['class'], $queueList);
                            self::writeError($server, $queue['class'], $e);
                        }
                    }
                    if (count($newQueueList) <> count($queueList)) $queueList = $newQueueList;
                });
                self::output(count($queueList) . " queues initialized");
            } else {
                self::output("0 queues initialized");
            }
            
        }
    }
    public static function onTask($server, int $task_id, int $src_worker_id, $data)
    {
        $queue = $data['class']::instance();
        try {
            $result = $queue->handle($data['msg']['data'], $data['msg']);
        } catch(\Throwable $th) {
            $result = false;
        }
        $data = [
            'class' => $data['class'],
            'msg' => $data['msg'],
            'result' => $result,
        ];
        $server->finish($data);
    }

    public static function onWorkerExit($server, int $workerId)
    {
        Timer::clearAll();
    }

    /**
     * 异常恢复
     * @param   \Swoole\server  $server
     * @param   string  $class
     * @param   array   $queueList
     */
    protected static function errorRecover($server, $class, &$queueList)
    {
        if ($server->config['error_recover_time'] > 0) {
            $server->after($server->config['error_recover_time'] * 1000, function() use($server, $class, &$queueList){
                try {
                    $queue = $class::instance();
                    if (!$queue->getEnable()) return;
                    $queueList[] = [
                        'class' => $class,
                        'queue' => $queue,
                        'consumer' => $queue->createConsumer(),
                    ];
                } catch(\Exception $e) {
                    self::errorRecover($server, $class, $queueList);
                    self::writeError($server, $class, $e);
                }
            });
        }
    }

    /**
     * 写入日志
     * @param   \Swoole\Server
     * @param   array   $data
     */
    protected static function writeLog($server, $data)
    {
        if (!empty($server->config['log_path'])) {
            if (!is_dir($server->config['log_path'])) mkdir($server->config['log_path'], 0777, true);
            $fileName = date('Y-m-d') . '.log';
            $queue = $data['class']::instance();
            $file = fopen("{$server->config['log_path']}/{$fileName}", 'a');
            flock($file, LOCK_EX);
            fwrite($file, str_pad('', 50, '-') . PHP_EOL);
            fwrite($file, "[ TIME ] " . date('Y-m-d H:i:s') . PHP_EOL);
            fwrite($file, "[ CLASS ] {$data['class']}" . PHP_EOL);
            fwrite($file, "[ TOPIC ] {$queue->getTopic()}" . PHP_EOL);
            fwrite($file, "[ QUEUE ] {$queue->getQueue()}" . PHP_EOL);
            fwrite($file, "[ MESSAGE ] " . var_export($data['msg'], true) . PHP_EOL);
            fwrite($file, "[ RESULT ] " . ($data['result'] !== false ? 'TRUE' : 'FALSE') . PHP_EOL);
            flock($file, LOCK_UN);
            fclose($file);
        }
    }

    /**
     * 写入异常日志
     * @param   \Swoole\server
     * @param   string  $queue
     * @param   \Exception  $e
     */
    protected static function writeError($server, $queue, $e)
    {
        if (!empty($server->config['log_path'])) {
            if (!is_dir($server->config['log_path'])) mkdir($server->config['log_path'], 0777, true);
            $fileName = date('Y-m-d') . '_error.log';
            $file = fopen($server->config['log_path'] . $fileName, 'a');
            flock($file, LOCK_EX);
            fwrite($file, str_pad('', 50, '-') . PHP_EOL);
            fwrite($file, "[ TIME ] " . date('Y-m-d H:i:s') . PHP_EOL);
            fwrite($file, "[ CLASS ] {$queue}" . PHP_EOL);
            fwrite($file, "[ MESSAGE ] {$e->getMessage()}" . PHP_EOL);
            fwrite($file, "[ EXCEPTION ] {$e}" . PHP_EOL);
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
