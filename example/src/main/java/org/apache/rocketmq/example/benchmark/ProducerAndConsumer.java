/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.benchmark;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;

import static org.apache.rocketmq.example.benchmark.UniformRateLimiter.uninterruptibleSleepNs;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 */
public class ProducerAndConsumer {
    private static DefaultMQAdminExt rmqAdmin;
    private static volatile UniformRateLimiter rateLimiter = new UniformRateLimiter(1.0);
    private static volatile AtomicInteger windowsSize = new AtomicInteger(512);
    private static volatile AtomicLong produceCnt = new AtomicLong(0);

    public static void createTopic(final String topic, final int partitions, boolean bcq) {
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setOrder(false);
        topicConfig.setPerm(6);
        topicConfig.setReadQueueNums(partitions);
        topicConfig.setWriteQueueNums(partitions);
        topicConfig.setTopicName(topic);

        if(bcq) {
            topicConfig.getAttributes().put("+" + TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getName(), "BatchCQ");
        }
        System.out.println(String.format("Create topic [%s]", topic));
        try {
            Set<String> brokerList = CommandUtil.fetchMasterAddrByClusterName(rmqAdmin, "DefaultCluster");
            topicConfig.setReadQueueNums(Math.max(1, partitions / brokerList.size()));
            topicConfig.setWriteQueueNums(Math.max(1, partitions / brokerList.size()));

            for (String brokerAddr : brokerList) {
//                brokerAddr = brokerAddr.replaceAll("10.0.1.17","116.62.189.249");
                rmqAdmin.createAndUpdateTopicConfig(brokerAddr, topicConfig);
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to create topic [%s] to cluster [%s]", topic, "DefaultCluster"), e);
        }
    }

    public static void main(String[] args) throws MQClientException, InterruptedException, MQBrokerException, RemotingException {
        String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
        if(namesrvAddr == null){
            System.out.println("namesrv is null");
            System.exit(0);
        }
//        String namesrvAddr = "54.169.148.224:9876";
        String topicName = "Topic-UUID";
        String groupName = "Group-UUID";
        int publishRate = 1000;
        boolean bcq = false;
        boolean autoBatch = false;
        boolean isConsume = true;
        int partition = 1;
        int i = 0;
        if (args.length > i) {
            publishRate = Integer.valueOf(args[i++]);
            if (args.length > i) {
                bcq = Boolean.valueOf(args[i++]);
            }
            if (args.length > i) {
                autoBatch = Boolean.valueOf(args[i++]);
            }
            if (args.length > i) {
                isConsume = Boolean.valueOf(args[i++]);
            }
            if (args.length > i) {
                partition = Integer.valueOf(args[i++]);
            }
            if (args.length > i) {
                topicName = args[i++];
            }
            if (args.length > i) {
                groupName = args[i++];
            }
        }
        if(groupName.equals("Group-UUID")) {
            groupName = "Group-" + UUID.randomUUID();
        }
        System.out.println("publishRate: " + publishRate);
        System.out.println("bcq: " + bcq);
        System.out.println("autoBatch: " + autoBatch);
        System.out.println("isConsume: " + isConsume);
        System.out.println("partition: " + partition);
        System.out.println("topicName: " + topicName);
        System.out.println("groupName: " + groupName);
        System.out.println("namesrvAddr: " + namesrvAddr);

        if(topicName.equals("Topic-UUID")) {
            topicName = "Topic-" + UUID.randomUUID();
            rmqAdmin = new DefaultMQAdminExt();
            rmqAdmin.setNamesrvAddr(namesrvAddr);
            rmqAdmin.setInstanceName("AdminInstance-" + UUID.randomUUID());
            try {
                rmqAdmin.start();
            } catch (MQClientException e) {
                System.out.println("Start the RocketMQ admin tool failed.");
            }

            while (true) {
                try {
                    createTopic(topicName, partition, bcq);
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        AtomicLong consumerCnt = new AtomicLong(0);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topicName, "*");
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                consumerCnt.addAndGet(msgs.size());
//                System.out.println("收到消息条数: " + msgs.size());
//                System.out.println("第一条消息的位点: " + msgs.get(0).getQueueOffset());
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        if (isConsume) {
            consumer.start();
            System.out.printf("Consumer Started.%n");
        }


        if(publishRate > 0) {
            DefaultMQProducer producer = new DefaultMQProducer(groupName);
            producer.setNamesrvAddr(namesrvAddr);
            producer.start();

            producer.setAutoBatch(autoBatch);
            producer.batchMaxDelayMs(30);
            producer.batchMaxBytes(32 * 1024);
            producer.totalBatchMaxBytes(2 * 64 * 1024 * 1024);

            byte[] payload = new byte[1024];
            new Random().nextBytes(payload);


            long startTime = System.currentTimeMillis();
            long prevTime = 0;

            rateLimiter = new UniformRateLimiter(publishRate);
            while (true) {
                try {
                    synchronized (ProducerAndConsumer.windowsSize) {
                        while (ProducerAndConsumer.windowsSize.get() <= 0) {
                            Thread.yield();
                        }
                    }
                    ProducerAndConsumer.windowsSize.decrementAndGet();

                    long nowTime = System.currentTimeMillis();
                    if (nowTime > startTime + 15 * 60 * 1000) {
                        break;
                    }
                    if (nowTime > prevTime + 1000) {
                        prevTime = nowTime;
                        System.out.println(String.format("produce: %d, consume: %d\nconsumeBatch: %d", produceCnt.longValue(), consumerCnt.longValue(), MessageDecoder.consumerBatchSize.longValue()));

                        produceCnt.set(0);
                        consumerCnt.set(0);
                    }
                    final long intendedSendTime = rateLimiter.acquire();
                    uninterruptibleSleepNs(intendedSendTime);

                    Message msg = new Message(topicName, payload);
                    producer.send(msg, new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            ProducerAndConsumer.produceCnt.incrementAndGet();
                            ProducerAndConsumer.windowsSize.incrementAndGet();
                        }

                        @Override
                        public void onException(Throwable e) {
                            ProducerAndConsumer.windowsSize.incrementAndGet();
                            throw new RuntimeException(e);
                        }
                    });
                } catch (Throwable e) {
                    e.printStackTrace();
                    Thread.sleep(1000);
                }

//            Message msg = new Message(topicName, payload);
//            List<Message> messages = new ArrayList<>();
//            messages.add(msg);
//            messages.add(msg);
//            messages.add(msg);
////
//            producer.send(messages, new SendCallback() {
//                @Override
//                public void onSuccess(SendResult sendResult) {
//                    System.out.println(sendResult);
//                }
//
//                @Override
//                public void onException(Throwable e) {
//
//                }
//            });
//            System.out.println(String.format("consume: %d\nconsumeBatch: %d", consumerCnt.longValue(), MessageDecoder.consumerBatchSize.longValue()));
//            consumerCnt.set(0);
//            Thread.sleep(1000);
            }
        }else{
            long startTime = System.currentTimeMillis();
            long prevTime = 0;

            while (true) {
                long nowTime = System.currentTimeMillis();
                if (nowTime > startTime + 15 * 60 * 1000) {
                    break;
                }
                if (nowTime > prevTime + 1000) {
                    prevTime = nowTime;
                    System.out.println(String.format("produce: %d, consume: %d\nconsumeBatch: %d", produceCnt.longValue(), consumerCnt.longValue(), MessageDecoder.consumerBatchSize.longValue()));

                    produceCnt.set(0);
                    consumerCnt.set(0);
                }
            }
        }
        /*
         * Shut down once the producer instance is not longer in use.
         */
//        producer.shutdown();
    }
}

