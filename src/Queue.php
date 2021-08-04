<?php
/*
 * @Description   队列抽象类
 * @Author        lifetime
 * @Date          2021-07-19 14:41:05
 * @LastEditTime  2021-08-04 16:33:18
 * @LastEditors   lifetime
 */
namespace swooleamqp;

use Enqueue\AmqpExt\AmqpConnectionFactory;
use Interop\Amqp\Impl\AmqpBind;

/**
 * 队列抽象类
 * @class Queue
 */
abstract class Queue
{
    /**
     * 启用
     * @var boolean
     */
    protected $enable = true;
    /**
     * 连接参数
     * @var array
     */
    protected $connectParams = [
        'host' => 'localhost',
        'port' => 5672,
        'user'=> '',
        'pass' => '',
        'vhost' => ''
    ];
    /**
     * 交换器名称
     * @var string
     */
    protected $topic = '';
    /**
     * 交换器 flags
     * @var array
     */
    protected $topicFlags = [];
    /**
     * 交换器类型
     * @var string
     */
    protected $topicType = '';
    /**
     * 队列名称
     * @var string
     */
    protected $queue = '';
    /**
     * 队列 flags
     * @var array
     */
    protected $queueFlags = [];
    /**
     * 消息路由Key
     * @var string
     */
    protected $msgRoutingKey = '';
    /**
     * 消费者绑定的路由Key
     * @var string
     */
    protected $bindRoutingKey = '';

    /**
     * 业务逻辑
     * @param   mixed   $data
     * @param   array   $message
     * @return  boolean
     */
    abstract public function handle($data, $message);

    /**
     * 实例列表
     * @var $this
     */
    protected static $instances;

    /**
     * 连接实例列表
     * @var array
     */
    protected $connects;

    /**
     * 连接实例
     * @var \Enqueue\AmqpExt\AmqpContext
     */
    protected $connect;

    /**
     * 延时时间 秒
     * @var int
     */
    protected $delay = 0;

    /**
     * 静态实例化
     * @return $this
     */
    public static function instance()
    {
        $key = md5(get_called_class());
        if (isset(self::$instances[$key])) return self::$instances[$key];
        return self::$instances[$key] = new static;
    }

    /**
     * 构造函数
     */
    private function __construct()
    {
    }

    /**
     * 是否启用
     * @return  boolean
     */
    public function getEnable()
    {
        return $this->enable == true;
    }

    /**
     * 获取交换机名称
     * @return  string
     */
    public function getTopic()
    {
        return $this->topic;
    }

    /**
     * 获取队列名称
     * @return string
     */
    public function getQueue()
    {
        return $this->queue;
    }
    
    /**
     * 构建单例连接
     */
    protected function buildConnect()
    {
        $key = md5(get_called_class() . serialize($this->connectParams));
        if (isset($this->connects[$key])) {
            $this->connect = $this->connects[$key];
        } else {
            $factory = new AmqpConnectionFactory($this->connectParams);
            $this->connect = $factory->createContext();
            $this->connects[$key] = $this->connect;
        }
    }

    /**
     * 创建 Topic
     * @return  \Interop\Amqp\AmqpTopic|null
     */
    protected function createTopic()
    {
        $topic = $this->connect->createTopic($this->topic);
        if (!empty($this->topicFlags)) {
            foreach($this->topicFlags as $flag) $topic->addFlag($flag);
        }
        if (!empty($this->topicType)) {
            $topic->setType($this->topicType);
        }
        $this->connect->declareTopic($topic);
        return $topic;
    }

    /**
     * 创建 Queue
     * @return  \Interop\Amqp\AmqpQueue
     */
    protected function createQueue()
    {
        $queue = $this->connect->createQueue($this->queue);
        if (!empty($this->queueFlags)) {
            foreach($this->queueFlags as $flag) $queue->addFlag($flag);
        }
        $this->connect->declareQueue($queue);
        return $queue;
    }

    /**
     * 创建消费者
     * @return \Enqueue\AmqpExt\AmqpConsumer
     */
    public function createConsumer()
    {
        $this->buildConnect();
        $topic = $this->createTopic();
        $queue = $this->createQueue();
        $this->connect->bind(new AmqpBind($topic, $queue, $this->bindRoutingKey));
        return $this->connect->createConsumer($queue);
    }

    /**
     * 设置延时时间
     * @param   int $delay
     * @return  $this
     */
    public function setDelay($delay)
    {
        $this->delay = $delay;
        return $this;
    }

    /**
     * 发送消息
     * @param   mixed   $msg    消息
     */
    public function send($msg)
    {
        $msgData = [
            'uuid' => uniqid($this->topic),
            'data' => $msg,
            'class' => get_called_class(),
            'time' => time(),
            'delay' => $this->delay * 1000
        ];
        $this->buildConnect();
        $message = $this->connect->createMessage(serialize($msgData));
        if (!empty($this->msgRoutingKey)) {
            $message->setRoutingKey($this->msgRoutingKey);
        }
        $producer = $this->connect->createProducer();
        if ($this->delay > 0) {
            $producer->setDelayStrategy(new DelayStrategy());
            $producer->setDeliveryDelay($this->delay * 1000);
        }
        $topic = $this->createTopic();
        $producer->send($topic, $message);
    }
}

