package umb.common;

import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class Helper3 {
    // four bytes topic and 4 bytes len (excluding the 4 bytes of len itself)
    public static int BUF_SZ = 20480;
    public static final int ACK_SZ = 8;

    public static class IpPort {
        public String ip;
        public int port;

        public IpPort(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }
    }

    public static class ServerThreadShared {
        public ArrayBlockingQueue<SocketChannel> newProducerConnections = new ArrayBlockingQueue<>(10000);
        public ArrayBlockingQueue<SocketChannel> newReplicaConnections = new ArrayBlockingQueue<>(10000);
    }


    public static class ServerThreadState {
        public Selector selector;
        public long hundredMillis;
        public long now;
        public ServerThreadShared shared;
        List<ProduceState> produceStates = new ArrayList<>();
        List<ReplicaConsumeState> replicaConsumeStates = new ArrayList<>();
        List<ReplicaProduceState> replicaProduceStates = new ArrayList<>();
        public List<SocketChannel> replicas = new ArrayList<>();

        public ServerThreadState(Selector selector, long hundredMillis, long now, ServerThreadShared shared) {
            this.selector = selector;
            this.hundredMillis = hundredMillis;
            this.now = now;
            this.shared = shared;
        }
    }

    public static class AppException extends Exception {
        public AppException() {
        }

        public AppException(String message) {
            super(message);
        }

        public AppException(String message, Throwable cause) {
            super(message, cause);
        }

        public AppException(Throwable cause) {
            super(cause);
        }

        public AppException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }

    public static class MessageState {
        /*
        len is the message length
        read is the amount of bytes of the current message read until now
         */
        public ByteBuffer current = ByteBuffer.allocate(BUF_SZ);
        public ByteBuffer replicaReply = ByteBuffer.allocate(ACK_SZ);
    }


    public static class ProduceState {
        public Integer topic = 3;
        public SocketChannel sc;
        public ServerThreadState threadState;
        public MessageState msgState = new MessageState();

        public ProduceState(SocketChannel sc, ServerThreadState threadState) {
            this.sc = sc;
            this.threadState = threadState;
        }
    }

    public static class ReplicaConsumeState {
        public Integer topic = 3;
        public SocketChannel sc;
        public ServerThreadState threadState;
        public MessageState msgState = new MessageState();

        public ReplicaConsumeState(SocketChannel sc, ServerThreadState threadState) {
            this.sc = sc;
            this.threadState = threadState;
        }
    }
    public static class ReplicaProduceState {
        SocketChannel sc;
    }
}
