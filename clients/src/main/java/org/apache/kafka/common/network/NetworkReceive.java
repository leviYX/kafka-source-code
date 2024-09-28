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

import org.apache.kafka.common.memory.MemoryPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;

/**
 * A size delimited Receive that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
public class NetworkReceive implements Receive {

    public final static String UNKNOWN_SOURCE = "";
    public final static int UNLIMITED = -1;
    private static final Logger log = LoggerFactory.getLogger(NetworkReceive.class);
    // 空的ByteBuffer
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private final String source;
    // 存储响应消息数据长度的ByteBuffer，长度4字节，完全可以符合消息长度，哪有那么大的消息
    private final ByteBuffer size;
    // 响应消息数据的最大长度
    private final int maxSize;
    // 内存池，其实本质是个ByteBuffer
    private final MemoryPool memoryPool;
    // 已读取字节大小
    private int requestedBufferSize = -1;
    // 存储响应消息数据体的ByteBuffer
    private ByteBuffer buffer;


    public NetworkReceive(String source, ByteBuffer buffer) {
        this.source = source;
        this.buffer = buffer;
        this.size = null;
        this.maxSize = UNLIMITED;
        this.memoryPool = MemoryPool.NONE;
    }

    public NetworkReceive(String source) {
        // 对应的channel id，即channel.id()
        this.source = source;
        // 初始化4字节给size，响应消息的数据长度，4字节就是int的长度
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        // 能接受消息的最大长度
        this.maxSize = UNLIMITED;
        // 内存池，其实本质是个ByteBuffer
        this.memoryPool = MemoryPool.NONE;
    }

    public NetworkReceive(int maxSize, String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = maxSize;
        this.memoryPool = MemoryPool.NONE;
    }

    public NetworkReceive(int maxSize, String source, MemoryPool memoryPool) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = maxSize;
        this.memoryPool = memoryPool;
    }

    public NetworkReceive() {
        this(UNKNOWN_SOURCE);
    }

    @Override
    public String source() {
        return source;
    }

    @Override
    public boolean complete() {
        // 消息头已经读完了(被rewin了)，&& 消息体也读完了
        return !size.hasRemaining() && buffer != null && !buffer.hasRemaining();
    }

    /**
     * 读取的时候，先读取4字节到size中，再根据size的大小为buffer分配空间，然后读满整个buffer就表示读完了
     * @param channel The channel to read from
     * @return
     * @throws IOException
     */
    public long readFrom(ScatteringByteChannel channel) throws IOException {
        // 读取数据的总大小，开始为0，还没读呢
        int read = 0;
        // 判断响应的消息是不是读完了，是否还有数据在buffer中
        if (size.hasRemaining()) {
            // 读取消息长度，就是全部读取，返回读取的长度字节数
            int bytesRead = channel.read(size);
            if (bytesRead < 0)
                throw new EOFException();
            // 累计读取字节数
            read += bytesRead;
            // 判断是否读取完了，如果没有，则继续读取
            if (!size.hasRemaining()) {
                /**
                 * 这个操作会重置position，将position置为0。nio知识，position和limit的概念，
                 * position指向当前可以操作的位置，limit指向数据尽头下一位，之间的距离就是你能操作的范围，capacity则永远不动，没啥讨论意义
                 * 因为上面处于写模式，数据被写进来，然后此时要读取数据了，所以重置position为0，此时limit还处于数据的尽头下一位，
                 * 于是此时buffer一共4字节，假如写入了2字节数据，此时position为0，limit为2，capacity为4
                 * 这里只是重置了size，后续他还继续用。
                 */
                size.rewind();
                // 随后他读取了int长度，其实就是四字节，把buffer读尽了，拿出来的就是本次消息的长度
                int receiveSize = size.getInt();
                if (receiveSize < 0)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + ")");
                if (maxSize != UNLIMITED && receiveSize > maxSize)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + " larger than " + maxSize + ")");
                // 将读取到的数据长度赋值给requestedBufferSize，赋值给已读字节大小，即数据体的大小
                requestedBufferSize = receiveSize; //may be 0 for some payloads (SASL)
                if (receiveSize == 0) {
                    buffer = EMPTY_BUFFER;
                }
            }
        }
        // 如果数据体buffer没分配，且响应消息数据头已读完，此时要分配requestedBufferSize字节大小的内存空间给数据体buffer
        if (buffer == null && requestedBufferSize != -1) { //we know the size we want but havent been able to allocate it yet
            buffer = memoryPool.tryAllocate(requestedBufferSize);
            if (buffer == null)
                log.trace("Broker low on memory - could not allocate buffer of size {} for source {}", requestedBufferSize, source);
        }
        // buffer分配成功，则继续读取数据体，因为buffer是根据sizeLi面的消息长度分配的，所以他能获取全部的消息，直接一次读完
        if (buffer != null) {
            int bytesRead = channel.read(buffer);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
        }

        return read;
    }

    @Override
    public boolean requiredMemoryAmountKnown() {
        return requestedBufferSize != -1;
    }

    @Override
    public boolean memoryAllocated() {
        return buffer != null;
    }


    @Override
    public void close() throws IOException {
        if (buffer != null && buffer != EMPTY_BUFFER) {
            memoryPool.release(buffer);
            buffer = null;
        }
    }

    public ByteBuffer payload() {
        return this.buffer;
    }

    public int bytesRead() {
        if (buffer == null)
            return size.position();
        return buffer.position() + size.position();
    }

    /**
     * Returns the total size of the receive including payload and size buffer
     * for use in metrics. This is consistent with {@link NetworkSend#size()}
     * 返回消息体还有多少没读+消息头还有多少没读
     */
    public int size() {
        return payload().limit() + size.limit();
    }

}
