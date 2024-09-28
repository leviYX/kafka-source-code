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

/*
 * Transport layer for PLAINTEXT communication
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;

import java.security.Principal;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class PlaintextTransportLayer implements TransportLayer {
    private final SelectionKey key;
    private final SocketChannel socketChannel;
    private final Principal principal = KafkaPrincipal.ANONYMOUS;

    public PlaintextTransportLayer(SelectionKey key) throws IOException {
        this.key = key;
        this.socketChannel = (SocketChannel) key.channel();
    }

    @Override
    public boolean ready() {
        return true;
    }

    /**
     * 判断sc连接是否完成
     * @return
     * @throws IOException
     */
    @Override
    public boolean finishConnect() throws IOException {
        // 判断连接是否完成
        boolean connected = socketChannel.finishConnect();

        /**
         * 如果完成，则设置key的interestOps为OP_READ开始监听read事件，并且删除对于OP_CONNECT事件的关注，因为如果不删除后面遍历的时候。
         * 还会遍历到这个事件，但是因为没有这个事件的发生，会有空指针异常。～表示按位取反，&表示按位与，|表示按位或。所以这个
         * ~SelectionKey.OP_CONNECT表示对OP_CONNECT取反，然后再和key.interestOps()就等于移除了OP_CONNECT事件
         * &～a就表示删除a事件，有就删除，没有不改变。而｜a表示把a事件添加进去
         */
        if (connected)
            key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
        // 返回连接是否完成
        return connected;
    }

    @Override
    public void disconnect() {
        //
        key.cancel();
    }

    @Override
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @Override
    public SelectionKey selectionKey() {
        return key;
    }

    @Override
    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    @Override
    public boolean isConnected() {
        return socketChannel.isConnected();
    }

    @Override
    public void close() throws IOException {
        socketChannel.socket().close();
        socketChannel.close();
    }

    /**
     * Performs SSL handshake hence is a no-op for the non-secure
     * implementation
     */
    @Override
    public void handshake() {}

    /**
    * Reads a sequence of bytes from this channel into the given buffer.
    *
    * @param dst The buffer into which bytes are to be transferred
    * @return The number of bytes read, possible zero or -1 if the channel has reached end-of-stream
    * @throws IOException if some other I/O error occurs
    */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        // sc读取数据到dst
        return socketChannel.read(dst);
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffers.
     *
     * @param dsts - The buffers into which bytes are to be transferred.
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream.
     * @throws IOException if some other I/O error occurs
     */
    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return socketChannel.read(dsts);
    }

    /**
     * Reads a sequence of bytes from this channel into a subsequence of the given buffers.
     * @param dsts - The buffers into which bytes are to be transferred
     * @param offset - The offset within the buffer array of the first buffer into which bytes are to be transferred; must be non-negative and no larger than dsts.length.
     * @param length - The maximum number of buffers to be accessed; must be non-negative and no larger than dsts.length - offset
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream.
     * @throws IOException if some other I/O error occurs
     */
    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return socketChannel.read(dsts, offset, length);
    }

    /**
    * Writes a sequence of bytes to this channel from the given buffer.
    *
    * @param src The buffer from which bytes are to be retrieved
    * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
    * @throws IOException If some other I/O error occurs
    */
    @Override
    public int write(ByteBuffer src) throws IOException {
        /**
         * 这个方法比较复杂，它会尝试从src中读取数据，直到src中的数据全部被读取完毕或者出现异常。
         * 复杂的地方在于我们在读取数据过来的数据，需要考虑半包和黏包问题，在netty中有解码器来实现这个问题，但是kafka没有这个解码器，
         * 他实现了NetworkReceive.receive()方法来处理半包和黏包问题，以及NetworkSend.send()方法来处理半包和黏包问题。
         */
        return socketChannel.write(src);
    }

    /**
    * Writes a sequence of bytes to this channel from the given buffer.
    *
    * @param srcs The buffer from which bytes are to be retrieved
    * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
    * @throws IOException If some other I/O error occurs
    */
    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return socketChannel.write(srcs);
    }

    /**
    * Writes a sequence of bytes to this channel from the subsequence of the given buffers.
    *
    * @param srcs The buffers from which bytes are to be retrieved
    * @param offset The offset within the buffer array of the first buffer from which bytes are to be retrieved; must be non-negative and no larger than srcs.length.
    * @param length - The maximum number of buffers to be accessed; must be non-negative and no larger than srcs.length - offset.
    * @return returns no.of bytes written , possibly zero.
    * @throws IOException If some other I/O error occurs
    */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return socketChannel.write(srcs, offset, length);
    }

    /**
     * always returns false as there will be not be any
     * pending writes since we directly write to socketChannel.
     */
    @Override
    public boolean hasPendingWrites() {
        return false;
    }

    /**
     * Returns ANONYMOUS as Principal.
     */
    @Override
    public Principal peerPrincipal() {
        return principal;
    }

    /**
     * Adds the interestOps to selectionKey.
     * key.interestOps() | ops来给channel注册感兴趣的事件，比如OP_READ，OP_WRITE，OP_CONNECT
     */
    @Override
    public void addInterestOps(int ops) {
        key.interestOps(key.interestOps() | ops);

    }

    /**
     * Removes the interestOps from selectionKey.
     */
    @Override
    public void removeInterestOps(int ops) {
        key.interestOps(key.interestOps() & ~ops);
    }

    @Override
    public boolean isMute() {
        return key.isValid() && (key.interestOps() & SelectionKey.OP_READ) == 0;
    }

    @Override
    public boolean hasBytesBuffered() {
        return false;
    }

    @Override
    public long transferFrom(FileChannel fileChannel, long position, long count) throws IOException {
        return fileChannel.transferTo(position, count, socketChannel);
    }
}
