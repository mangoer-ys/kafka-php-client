<?php
#================================================================
# Author           : Mangoer
# Email            : yangshuai-g@360.com
# Last modified    : 2017-03-21 16:42
# Filename         : KafkaConfig.php
# Description      :
#
#================================================================


class KafkaConfig
{
    const BROKERLIST   = '10.142.100.51:9092,10.142.100.52:9092,10.142.107.71:9092';
    const OFFSET_COMMIT_INTERVAL = 30;

    public $group;
    public $logger;
    public $offsetReset;

    public static function init($group, $logger, $offsetReset = 'smallest')
    {
        $khydraConf = new KafkaConfig();
        $khydraConf->group  = $group;
        $khydraConf->logger = $logger;
        $khydraConf->offsetReset = $offsetReset;

        return $khydraConf;
    }

    public static function getConsumerConf($group, $offsetReset)
    {
        if (empty($group))
        {
            $group = 'defaultGroup';
        }

        $conf = new RdKafka\Conf();
        $conf->set('metadata.broker.list', KafkaConfig::BROKERLIST);
        $conf->set('enable.auto.commit', 'false');
        $conf->set('group.id', $group);
        $conf->setDefaultTopicConf(KafkaConfig::getTopicConf($offsetReset));

        $conf->setRebalanceCb(function (RdKafka\KafkaConsumer $kafka, $err, array $partitions = null)
        {
            switch ($err)
            {
            case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                $kafka->assign($partitions);
                break;

            case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                $kafka->assign(NULL);
                break;

            default:
                $kafka->assign(NULL);
                $this->logger->error("kafka rebalance error: $err");
            }
        });

        return $conf;
    }

    public static function getTopicConf($offsetReset)
    {
        $topicConf = new RdKafka\TopicConf();
        $topicConf->set('auto.offset.reset', $offsetReset);
        $topicConf->set('offset.store.method', 'broker');

        return $topicConf;
    }
}
