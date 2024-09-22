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
package org.apache.kafka.common.utils;

import org.apache.kafka.common.errors.TimeoutException;

import java.util.function.Supplier;

/**
 * A time implementation that uses the system clock and sleep call. Use `Time.SYSTEM` instead of creating an instance
 * of this class.
 */
public class SystemTime implements Time {

    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
        Utils.sleep(ms);
    }

    @Override
    public void waitObject(Object obj, Supplier<Boolean> condition, long deadlineMs) throws InterruptedException {
        // 加锁obj，obj就是ProducerMetadata，然后循环等待，直到满足条件或者超时
        synchronized (obj) {
            while (true) {
                /**
                 * 调用第二个参数的函数式的get方法，返回一个布尔值，如果返回true，则退出循环，这个函数表达式的内容为
                 * () -> {
                 *     maybeThrowFatalException();
                 *     return updateVersion() > lastVersion || isClosed();
                 *  }
                 *  含义就是有问题就抛出异常，没问题就去发起更新版本号的操作，返回一个
                 *  boolean值，如果返回true，表示获取成功，则退出循环，否则就走到下面的wait方法
                 *  阻塞到过期时间之前，然后继续进入while循环，这样可以避免cpu空转
                 *  同时这里阻塞这个过期时间，也不是就一直到过期时间，其他地方会发起唤醒
                 */
                if (condition.get())
                    return;
                // 如果当前时间已经超过了deadlineMs，则抛出超时异常
                long currentTimeMs = milliseconds();
                if (currentTimeMs >= deadlineMs)
                    throw new TimeoutException("Condition not satisfied before deadline");
                // 否则，等待deadlineMs - currentTimeMs毫秒，然后再次尝试检查条件
                obj.wait(deadlineMs - currentTimeMs);
            }
        }
    }

}
