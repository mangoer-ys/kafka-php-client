<?php
#================================================================
# Author           : Mangoer
# Email            : yangshuai-g@360.com
# Last modified    : 2017-03-17 14:25
# Filename         : ConsumerTest.php
# Description      : 
# 
#================================================================
 
require("KafkaConsumer.php");

testConusmer();

function testConusmer()
{
    $topic = "mangoer-ys";
    $func  = function($message) {
        var_dump($message);
    };

    $kafka_consumer = new KafkaConsumer();
    $kafka_consumer->subscribe($topic, $func);
    $kafka_consumer->consume();
}
