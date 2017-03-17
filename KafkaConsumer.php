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

    public function __construct()
    {
        $this->initConf();
        $this->consumer = new RdKafka\KafkaConsumer($this->conf);
    }

    public function subscribe($topic, $func)
    {
        $this->consumer->subscribe(array($topic));
        $this->func = $func;
    }

    public function consume()
    {
        while (true) 
        {
            $message = $this->consumer->consume(120*10000);
            switch ($message->err) 
            {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                call_user_func($this->func, $message);
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                echo "No more messages; will wait for more\n";
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                echo "Timed out\n";
                break;
            default:
                throw new \Exception($message->errstr(), $message->err);
                break;
            }
        }
    }

    private function initConf()
    {
        $this->conf = new RdKafka\Conf();

        $this->conf->set('group.id', 'Mangoer-ConsumerGroup');
        $this->conf->set('metadata.broker.list', '10.142.100.51:9092');
        $this->conf->setDefaultTopicConf($this->getTopicConf());

        $this->conf->setRebalanceCb(function (RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) 
            {
            case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                $kafka->assign($partitions);
                break;

            case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                $kafka->assign(NULL);
                break;

            default:
                throw new \Exception($err);
            }
        });
    }

    private function getTopicConf()
    {
        $topicConf = new RdKafka\TopicConf();
        $topicConf->set('auto.commit.interval.ms', 100);
        $topicConf->set('offset.store.method', 'file');
        $topicConf->set('offset.store.path', sys_get_temp_dir());
        $topicConf->set('auto.offset.reset', 'smallest');

        return $topicConf;
    }
}
