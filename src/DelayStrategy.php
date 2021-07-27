<?php
/*
 * @Description   延时驱动
 * @Author        lifetime
 * @Date          2021-07-26 17:36:59
 * @LastEditTime  2021-07-26 17:40:46
 * @LastEditors   lifetime
 */
namespace swooleamqp;

use Enqueue\AmqpTools\DelayStrategy as AmqpToolsDelayStrategy;
use Enqueue\AmqpTools\DelayStrategyAware;
use Interop\Amqp\AmqpContext;
use Interop\Amqp\AmqpDestination;
use Interop\Amqp\AmqpMessage;
use Interop\Amqp\AmqpQueue;
use Interop\Amqp\AmqpTopic;
use Interop\Amqp\Impl\AmqpBind;
use Interop\Queue\Exception\InvalidDestinationException;

/**
 * 延时驱动
 * @class DelayStrategy
 */
class DelayStrategy implements AmqpToolsDelayStrategy
{
    public function delayMessage(AmqpContext $context, AmqpDestination $dest, AmqpMessage $message, int $delay): void
    {
        $delayMessage = $context->createMessage($message->getBody(), $message->getProperties(), $message->getHeaders());
        $delayMessage->setProperty('x-delay', (int) $delay);
        $delayMessage->setRoutingKey($message->getRoutingKey());

        if ($dest instanceof AmqpTopic) {
            $delayTopic = $context->createTopic('enqueue.'.$dest->getTopicName().'.delayed');
            $delayTopic->setType('x-delayed-message');
            $delayTopic->addFlag($dest->getFlags());
            $delayTopic->setArgument('x-delayed-type', $dest->getType());

            $context->declareTopic($delayTopic);
            $context->bind(new AmqpBind($delayTopic, $dest, $delayMessage->getRoutingKey()));
        } elseif ($dest instanceof AmqpQueue) {
            $delayTopic = $context->createTopic('enqueue.queue.delayed');
            $delayTopic->setType('x-delayed-message');
            $delayTopic->addFlag(AmqpTopic::FLAG_DURABLE);
            $delayTopic->setArgument('x-delayed-type', AmqpTopic::TYPE_DIRECT);

            $delayMessage->setRoutingKey($dest->getQueueName());

            $context->declareTopic($delayTopic);
            $context->bind(new AmqpBind($delayTopic, $dest, $delayMessage->getRoutingKey()));
        } else {
            throw new InvalidDestinationException(sprintf('The destination must be an instance of %s but got %s.',
                AmqpTopic::class.'|'.AmqpQueue::class,
                get_class($dest)
            ));
        }

        $producer = $context->createProducer();

        if ($producer instanceof DelayStrategyAware) {
            $producer->setDelayStrategy(null);
        }

        $producer->send($delayTopic, $delayMessage);
    }
}