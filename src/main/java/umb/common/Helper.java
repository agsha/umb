package umb.common;

import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class Helper {
    // four bytes topic and 4 bytes len (excluding the 4 bytes of len itself)
    public static final int HEADER_LEN = 8;

    public static class IpPort {
        public String ip;
        public int port;

        public IpPort(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }
    }

    public static class ServerThreadShared {
        public ArrayBlockingQueue<SocketChannel> newProducerConnections = new ArrayBlockingQueue<>(100);
        public ArrayBlockingQueue<SocketChannel> newReplicaConnections = new ArrayBlockingQueue<>(100);
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
        public ByteBuffer replicaReply = ByteBuffer.allocate(1);

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
        public ByteBuffer header;
        public LinkedList<ByteBuffer> message = new LinkedList<>();
        /*
        len is the message length
        read is the amount of bytes of the current message read until now
         */
        public int len = 0;
        public int read = 0;
        public ByteBuffer[] osMessageCache = new ByteBuffer[1024];
        public ByteBuffer[] osMessage;
        public ByteBuffer current;
        public int readNow = 0;
        public HeaderState stateNow = HeaderState.HDR_INCOMPLETE;
        public int count = 0;

        public MessageState(int headerLen) {
            header = ByteBuffer.allocate(headerLen);
            current = ByteBuffer.allocate(1024);
            message.add(current);
        }


        public enum HeaderState {
            HDR_INCOMPLETE, HDR_JUST_FULL, HDR_ALREADY_FULL, MSG_COMPLETE, MSG_INCOMPLETE, INVALID
        }
    }


    public static class ProduceState {
        public Integer topic = 0;
        public SocketChannel sc;
        public ServerThreadState threadState;
        public MessageState msgState = new MessageState(HEADER_LEN);

        public ProduceState(int remaining, int topic, SocketChannel sc, ServerThreadState threadState) {
            this.topic = topic;
            this.sc = sc;
            this.threadState = threadState;
        }
    }

    public static class ReplicaConsumeState {
        SocketChannel sc;
    }
    public static class ReplicaProduceState {
        SocketChannel sc;
    }
}
