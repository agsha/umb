package umb.common;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class UmbSocketChannelImpl implements UmbSocketChannel{
    SocketChannel socketChannel;

    public UmbSocketChannelImpl() throws IOException {
        socketChannel = SocketChannel.open();
    }

    @Override
    public boolean connect(SocketAddress remote) throws IOException {
        return socketChannel.connect(remote);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return socketChannel.write(srcs, offset, length);
    }

    @Override
    public long write(ByteBuffer b) throws IOException {
        return socketChannel.write(b);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return socketChannel.read(dst);
    }

    @Override
    public void close() throws IOException {
        socketChannel.close();
    }
}
