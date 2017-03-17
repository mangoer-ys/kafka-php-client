<?php
#================================================================
# Author           : Mangoer
# Email            : yangshuai-g@360.com
# Last modified    : 2017-03-17 14:34
# Filename         : ProducerTest.php
# Description      : 
# 
#================================================================
 

require("KafkaProducer.php");

testProducer();

function testProducer()
{
    $producer = new KafkaProducer;
    $topic    = "mangoer";
    $message  = "i am mangoer";

    $producer->produce($topic, $message);
}
