<?php
#================================================================
# Author           : Mangoer
# Email            : yangshuai-g@360.com
# Last modified    : 2017-03-17 14:32
# Filename         : KafkaProducer.php
# Description      : 
# 
#================================================================
 

class KafkaProducer
{
    private $producer;
    private $topic;

    public function __construct()
    {
        $conf = new RdKafka\Conf();
        // $conf->setDrMsgCb($callback);

        $this->producer = new RdKafka\Producer($conf);
        $this->producer->setLogLevel(LOG_DEBUG);
        $this->producer->addBrokers("10.142.100.51:9092");
    }

    public function produce($topic, $message, $key = NULL)
    {
        $topic = $this->producer->newTopic($topic);
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message, $key);

        // $this->producer->poll(0);
    }
}
