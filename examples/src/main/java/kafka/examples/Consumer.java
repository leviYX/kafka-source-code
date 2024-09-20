/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer extends ShutdownableThread {
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;
    private final String groupId;
    private final int numMessageToConsume;
    private int messageRemaining;
    private final CountDownLatch latch;

    public Consumer(final String topic,
                    final String groupId,
                    final Optional<String> instanceId,
                    final boolean readCommitted,
                    final int numMessageToConsume,
                    final CountDownLatch latch) {
        super("KafkaConsumerExample", false);
        this.groupId = groupId;
        // 消费者配置参数集合
        Properties props = new Properties();
        // 填充消费者属性信息，包括服务器地址、组ID、是否自动提交、自动提交间隔、会话超时时间、键值反序列化器、值反序列化器、
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        instanceId.ifPresent(id -> props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, id));
        // 设置自动提交偏移量，自动提交间隔为1000毫秒，每次在调用kafkaconsumer.poll()方法时,都会检测是不是需要自动提交，并且提交上次poll
        // 方法返回的最后一个消息的offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 如果是自动提交，则设置读取已提交的消息
        if (readCommitted) {
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        /**
         * 创建初始化KafkaConsumer对象，这个对象负责与kafka集群进行通信，配置KafkaConsumer对象，包括指定kafka集群地址，消费组ID，
         * 消费组实例ID，是否自动提交，自动提交间隔，会话超时时间，键值反序列化器，值反序列化器等参数来控制他的行为准则
         * 位置位于org.apache.kafka.clients.consumer.KafkaConsumer#KafkaConsumer(ConsumerConfig, Deserializer, .Deserializer)
         */
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.numMessageToConsume = numMessageToConsume;
        this.messageRemaining = numMessageToConsume;
        this.latch = latch;
    }

    KafkaConsumer<Integer, String> get() {
        return consumer;
    }

    @Override
    public void doWork() {
        /**
         * 订阅一个或多个主题，通过调用subscribe方法，并且指定需要订阅的主题名称列表来实现，这个方法会发送一次订阅请求到kafka集群，
         * kafka会返回订阅成功的主题列表，如果订阅失败，则会抛出异常，位置位于
         * org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(java.util.Collection, ConsumerRebalanceListener)
         */
        consumer.subscribe(Collections.singletonList(this.topic));
        /**
         * 开始拉取数据，这个方法会向kafka集群发送拉取请求，然后等待kafka返回数据，返回的数据被存储在内存的缓冲区中，等待消费者处理
         */
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println(groupId + " received message : from partition " + record.partition() + ", (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
        }
        messageRemaining -= records.count();
        if (messageRemaining <= 0) {
            System.out.println(groupId + " finished reading " + numMessageToConsume + " messages");
            latch.countDown();
        }
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }
}
