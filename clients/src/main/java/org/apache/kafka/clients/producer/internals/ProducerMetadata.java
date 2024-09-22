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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ProducerMetadata extends Metadata {
    // If a topic hasn't been accessed for this many milliseconds, it is removed from the cache.
    // topic的元数据过期时间，如果这个时间段内没被访问这个topic，就会从缓存中删除，后面再用就得重新获取
    private final long metadataIdleMs;

    /* Topics with expiry time */
    /**
     * 主题元数据信息，里面存储主题和主题过期时间的对应关系，key是topic，value是topic的过期时间(nowMs+metadataIdleMs)
     * 过期则会被移除
     */
    private final Map<String, Long> topics = new HashMap<>();
    // 新的主题集合，第一次要发的主题，每次新的订阅会覆盖之前
    private final Set<String> newTopics = new HashSet<>();
    private final Logger log;
    private final Time time;

    public ProducerMetadata(long refreshBackoffMs,
                            long metadataExpireMs,
                            long metadataIdleMs,
                            LogContext logContext,
                            ClusterResourceListeners clusterResourceListeners,
                            Time time) {
        // 初始化的时候调用父类的构造函数完成初始化基础信息，ProducerMetadata是个子类，继承自Metadata
        super(refreshBackoffMs, metadataExpireMs, logContext, clusterResourceListeners);
        this.metadataIdleMs = metadataIdleMs;
        this.log = logContext.logger(ProducerMetadata.class);
        this.time = time;
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilder() {
        return new MetadataRequest.Builder(new ArrayList<>(topics.keySet()), true);
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilderForNewTopics() {
        return new MetadataRequest.Builder(new ArrayList<>(newTopics), true);
    }

    /**
     * 把获取到的元数据添加进来保存，往元数据主题集合topics中添加主题和对应的过期时间(当前时间+过期时间段，默认五分钟)
     * 如果元数据主题集合不存在该主题时，说明是第一次，那就把该主题添加到新主题集合中
     * 标记要更新主题元数据的属性字段lastRefreshMs=nowMs needPartialUpdate=true requestVersion 以便后续唤醒Sender线程去服务端拉取新主题的元数据
     * 此时主题信息就被添加到了元数据的主题集合中，此时要是过期了，retainTopic方法会做处理
     * @param topic
     * @param nowMs
     */
    public synchronized void add(String topic, long nowMs) {
        Objects.requireNonNull(topic, "topic cannot be null");
        if (topics.put(topic, nowMs + metadataIdleMs) == null) {
            // 把主题添加到新的主题集合中
            newTopics.add(topic);
            // 更新元数据标记，这是父类方法，直接调用即可
            requestUpdateForNewTopics();
        }
    }

    public synchronized int requestUpdateForTopic(String topic) {
        // 如果新主题集合存在该主题
        if (newTopics.contains(topic)) {
            // 只更新当前的主题元数据，并且返回的int为请求版本
            return requestUpdateForNewTopics();
        } else {
            // 如果不包含，就更新所有主题
            return requestUpdate();
        }
    }

    // Visible for testing
    synchronized Set<String> topics() {
        return topics.keySet();
    }

    // Visible for testing
    synchronized Set<String> newTopics() {
        return newTopics;
    }

    public synchronized boolean containsTopic(String topic) {
        return topics.containsKey(topic);
    }

    /**
     * 判断元数据中是不是应该保留该主题，会在handleMetadataResponse方法中调用，也就是在处理元数据响应结果的时候进行调用
     * 并且如果发现他过期了，这次请求元数据就不带他了，等下次请求他的时候再请求元数据，这样可以减少请求负载
     * @param topic
     * @param isInternal
     * @param nowMs
     * @return
     */
    @Override
    public synchronized boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        // 获取该主题的过期时间
        Long expireMs = topics.get(topic);
        // 为空说明该主题不在元数据主题集合中
        if (expireMs == null) {
            return false;
        }
        // 判断在不在新集合中
        else if (newTopics.contains(topic)) {
            return true;
        }
        // 判断是否过期，过期就从元数据主题集合中移除
        else if (expireMs <= nowMs) {
            log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", topic, expireMs, nowMs);
            topics.remove(topic);
            return false;
        } else {
            return true;
        }
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     */
    public synchronized void awaitUpdate(final int lastVersion, final long timeoutMs) throws InterruptedException {
        long currentTimeMs = time.milliseconds();
        long deadlineMs = currentTimeMs + timeoutMs < 0 ? Long.MAX_VALUE : currentTimeMs + timeoutMs;
        // 阻塞线程，其实现位于SystemTime的waitObject方法
        time.waitObject(this, () -> {
            // Throw fatal exceptions, if there are any. Recoverable topic errors will be handled by the caller.
            maybeThrowFatalException();
            return updateVersion() > lastVersion || isClosed();
        }, deadlineMs);

        if (isClosed())
            throw new KafkaException("Requested metadata update after close");
    }

    @Override
    public synchronized void update(int requestVersion, MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        // 调用父类的update方法，完成元数据更新
        super.update(requestVersion, response, isPartialUpdate, nowMs);

        // Remove all topics in the response that are in the new topic set. Note that if an error was encountered for a
        // new topic's metadata, then any work to resolve the error will include the topic in a full metadata update.
        // 如果新主题集合不为空，就遍历响应中的主题元数据，然后从新主题集合中移除该主题
        if (!newTopics.isEmpty()) {
            for (MetadataResponse.TopicMetadata metadata : response.topicMetadata()) {
                newTopics.remove(metadata.topic());
            }
        }
        // 更新之后唤醒等待元数据更新完成的线程，有唤醒就有阻塞，其阻塞的地方在awaitUpdate方法中
        notifyAll();
    }

    @Override
    public synchronized void fatalError(KafkaException fatalException) {
        super.fatalError(fatalException);
        notifyAll();
    }

    /**
     * Close this instance and notify any awaiting threads.
     */
    @Override
    public synchronized void close() {
        super.close();
        notifyAll();
    }

}
