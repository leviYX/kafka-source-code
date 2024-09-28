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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A send backed by an array of byte buffers
 */
public class ByteBufferSend implements Send {

    // 一共要写多少字节的数据
    private final long size;
    // 用于写入channel里的ByteBuffer数组，说明kafka一次最大传输字节是有限制的
    protected final ByteBuffer[] buffers;
    // 还剩下多少字节没有写
    private long remaining;
    private boolean pending = false;

    public ByteBufferSend(ByteBuffer... buffers) {
        this.buffers = buffers;
        for (ByteBuffer buffer : buffers)
            remaining += buffer.remaining();
        // 计算需要写入字节的总和
        this.size = remaining;
    }

    public ByteBufferSend(ByteBuffer[] buffers, long size) {
        this.buffers = buffers;
        this.size = size;
        this.remaining = size;
    }

    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    @Override
    public long size() {
        return this.size;
    }

    /**
     * 将字节流数据写入到channel中
     * @param channel The Channel to write to
     * @return
     * @throws IOException
     */
    @Override
    public long writeTo(TransferableChannel channel) throws IOException {
        // 将字节流数据写入到sc这个channel中，返回实际写入的字节数，因为网络传输可能一次写不完，所以需要知道写了多少长度
        long written = channel.write(buffers);
        if (written < 0)
            throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
        // 更新还剩下多少字节没有写
        remaining -= written;
        // 每次都检查一下
        pending = channel.hasPendingWrites();
        return written;
    }

    public long remaining() {
        return remaining;
    }

    @Override
    public String toString() {
        return "ByteBufferSend(" +
            ", size=" + size +
            ", remaining=" + remaining +
            ", pending=" + pending +
            ')';
    }

    public static ByteBufferSend sizePrefixed(ByteBuffer buffer) {
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(0, buffer.remaining());
        return new ByteBufferSend(sizeBuffer, buffer);
    }
}
