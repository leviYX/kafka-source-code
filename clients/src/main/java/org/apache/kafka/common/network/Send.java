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
package org.apache.kafka.common.network;

import java.io.IOException;

/**
 * This interface models the in-progress sending of data.
 */
public interface Send {

    /**
     * Is this send complete? 要发送的数据是否已经发送完成？
     */
    boolean completed();

    /**
     * Write some as-yet unwritten bytes from this send to the provided channel. It may take multiple calls for the send
     * to be completely written
     * @param channel The Channel to write to
     * @return The number of bytes written
     * @throws IOException If the write fails
     * 把数据写入对应的channel中
     */
    long writeTo(TransferableChannel channel) throws IOException;

    /**
     * Size of the send 发送数据的大小
     */
    long size();

}
