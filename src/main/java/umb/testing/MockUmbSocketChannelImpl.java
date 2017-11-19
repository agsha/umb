package umb.testing;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import umb.common.UmbSocketChannel;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

public class MockUmbSocketChannelImpl implements UmbSocketChannel {
    private static final Logger log = LoggerFactory.getLogger(MockUmbSocketChannelImpl.class);

    @Override
    public boolean connect(SocketAddress remote) throws IOException {
        return false;
    }

    long messageId = 0;
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        int sum = 0;
        for(int i=0; i<srcs.length; i++) {
            byte[] array = srcs[i].array();
            for(int j=0; j< array.length; j++) {
                sum+=array[j];
            }
        }
        if(sum==Integer.MAX_VALUE) {
            log.debug("unlucky");
        }
        messageId = srcs[0].getLong(0);
        return length;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        dst.put((byte)1);
        return 1;
    }

    @Override
    public void close() throws IOException {

    }
}
