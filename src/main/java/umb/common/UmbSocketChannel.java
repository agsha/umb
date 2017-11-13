package umb.common;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

public interface UmbSocketChannel {
    boolean connect(SocketAddress remote) throws IOException;

    long write(ByteBuffer[] srcs, int offset, int length)
            throws IOException;

    int read(ByteBuffer dst) throws IOException;

    void close() throws IOException;
}
