<?php
#================================================================
# Author           : Mangoer
# Email            : yangshuai-g@360.com
# Last modified    : 2017-03-16 20:50
# Filename         : consumer.php
# Description      :
#
#================================================================


class KafkaConsumer
{
    private $consumer;
    private $topic;
    private $func;

    public function __construct($config)
    {
        $this->group  = $config->group;
        $this->logger = $config->logger;
        assert(is_object($this->logger));
    }

    public function subscribe($topic, $func)
    {
        $this->func = $func;

        $conf = KafkaConfig::getConsumerConf($this->group);
        $this->consumer = new RdKafka\KafkaConsumer($conf);
        $this->consumer->subscribe(array($topic));
    }

    public function consume()
    {
        $count = 0;
        while (true)
        {
            $message = $this->consumer->consume(self::TIME_OUT);
            switch ($message->err)
            {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                $this->_process($message->payload);
                $this->_commitOffset($count, $message);
                break;

            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                break;

            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                break;

            default:
                $this->logger->error("kafka error: err={$message->err}, errstr=" . $message->errstr());
            }
        }
    }

    private function _process($message)
    {
        while (call_user_func($this->func, $message) === false);
    }

    private function _commitOffset(&$count, $message)
    {
        if (++$count >= KafkaConfig::OFFSET_COMMIT_INTERVAL)
        {
            $this->consumer->commit($message);
            $count = 0;
        }
    }
}
