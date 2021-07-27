# 基于swoole的RabbitMQ服务

本类库对 RabbitMQ 进行了一个简单的封装。使用的时候一行代码即可调用。

## `Queue` 抽象类

可以继承 `Queue` 类定义不同的队列

比如
```php
<?php

namespace queue;

use Interop\Amqp\AmqpQueue;
use Interop\Amqp\AmqpTopic;
use swooleamqp\Queue;

class MyQueue extends Queue
{
    // 连接参数
    protected $connectParams = [
        'host' => 'localhost',
        'port' => 5672,
        'user'=> 'test',
        'pass' => 'test',
        'vhost' => 'test'
    ];
    // 交换器名称
    protected $topic = 'swoole_amqp2';
    // 交换器类型
    protected $topicType = AmqpTopic::TYPE_TOPIC;
    // 交换器参数
    protected $topicFlags = [
        AmqpTopic::FLAG_DURABLE
    ];
    // 队列名称
    protected $queue = 'test1';
    // 队列参数
    protected $queueFlags = [
        AmqpQueue::FLAG_DURABLE
    ];
    // 消息路由 Key
    protected $msgRoutingKey = 'abc.edf';
    // 队列绑定路由 Key
    protected $bindRoutingKey = 'abc.#';
    /**
     * 逻辑函数
     * @param   mixed   $data       消息数据
     * @param   array   $message    完整的消息
    */
    public function handle($data, $message)
    {
        var_dump($data);
    }
}
```

> 目前不支持 headers 类型

## `Server` 基于 Swoole 的服务器

`Server` 类利用 Swoole 的服务器，对 RabbitMQ 的消息进行消费，然后执行对应的逻辑。

创建一个 `queue.php` 文件，将所有的 `Queue` 类返回。
```php
<?php

return [
    \queue\MyQueue::class,
];
```

启动服务器

```php
$config = [
    'port' => 9503,
    'task_worker_num' => 1,
    'pid_file' => '',
    'queue_file_path' => __DIR__ . "/queue.php",
    'log_path' => __DIR__ . "/log/queue/",
    'daemonize' => false,
    'hit_update' => true,
    'error_recover_time' => 30,
];
Server::instance($config)->start();
```

除了 `start` 之外，还有 `stop`、`reload`和`restart`方法。

> 注意：进行 stop、reload和restart操作的时候 一定要使配置参数与启动时一致。

配置参数

`port`  
swoole 服务器使用的端口  
`task_worker_num`  
任务进程数量  
`pid_file`  
PID文件路径  
`queue_file_path`  
队列列表文件路径  
`log_path`  
日志文件路径  
`daemonize`  
以守护进程的方式运行  
`hit_update`  
热更新 (开启此选项，当队列列表文件或队列类发生变更之后，服务器会自动载入新的内容，不再需要手动重启)  
`error_recover_time`  
异常恢复时间，单位：秒 (0 表示关闭此功能)；当消费者发生异常时，在指定时间后会重新创建此消费者。

## 生产消息

执行队列类的 `send` 方法即可生产消息。消息类型支持PHP所有可以序列化的类型。
```php
    \queue\MyQueue::instance()->send('hahhaha');
```

如果需要延时，可以调用 `setDelay` 方法，参数为延时的秒数。
```php
    \queue\MyQueue::instance()->setDelay(10 * 60)->send('hahhaha');
```