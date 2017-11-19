package umb.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sha.Utils;
import umb.client.UmbClient2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static umb.common.Helper.*;
import static umb.common.Helper.MessageState.HeaderState.*;

public class UmbServer {
    private static final Logger log = LoggerFactory.getLogger(UmbServer.class);
    private JsonNode config;
    private String id;
    private int producePort;
    private int consumerPort;
    private int replicaPort;
    private int threads;
    private final List<Thread> serverThreads;

    private final Thread producerListener;
    private final Thread replicaListener;
    private final List<ServerThreadShared> serverThreadShareds;
    HashMap<Integer, Integer> topicToReplicationFactor = new HashMap<>();
    CountDownLatch readyLatch = new CountDownLatch(2);

    public UmbServer(String[] args) {
        String conf = "config.json";
        int ii=0;
        while(ii < args.length) {
            if(args[ii].equals("conf")) {
                conf = args[ii+1];
            } else if(args[ii].equals("id")) {
                id = (args[ii+1]);
            }
            ii+=2;
        }
        config = Utils.readJsonFromClasspath(conf, JsonNode.class);

        log.debug("config file is {}", conf);
        JsonNode myConfig = config.get(id);
        producePort = myConfig.get("producePort").asInt();
        consumerPort = myConfig.get("consumerPort").asInt();
        replicaPort = myConfig.get("replicaPort").asInt();
        threads = myConfig.get("numServerThreads").asInt();

        ii=0;
        while(ii < args.length) {
            if(args[ii].equals("id")) {
                id = (args[ii+1]);
            } else if(args[ii].equals("threads")) {
                threads = Integer.parseInt(args[ii+1]);
            }
            ii+=2;
        }




        topicMetadata(config);
        serverThreads = new ArrayList<>();
        serverThreadShareds = new ArrayList<>();
        for(int i=0; i<threads; i++) {
            ServerThreadShared s = new ServerThreadShared();
            serverThreadShareds.add(s);
            int finalI = i;
            Thread t = new Thread(() -> {
                try {
                    Thread.currentThread().setName(String.format("Server-%s-serverThread-%d", id, finalI));
                    serverThread(s);
                } catch (Exception e) {
                    log.error("error in server thread", e);
                }
            });
            serverThreads.add(t);
            t.start();
        }
        producerListener = new Thread(() -> {
            try {
                producerListenerThread();
            } catch (IOException e) {
                log.error("error in producer thread", e);
            }
        });
        producerListener.start();

        replicaListener = new Thread(() -> {
            try {
                replicaListenerThread();
            } catch (IOException e) {
                log.error("error in replica thread", e);
            }
        });
        replicaListener.start();
        try {
            readyLatch.await();
        } catch (InterruptedException e) {
            log.error("what the f?", e);
        }
    }

    private void topicMetadata(JsonNode config) {
        ObjectNode on = (ObjectNode)config.get("topics");
        Iterator<Map.Entry<String, JsonNode>> fields = on.fields();
        while(fields.hasNext()) {
            Map.Entry<String, JsonNode> next = fields.next();
//            log.debug("keyyyy {}", next.getKey());
            Integer id = Integer.parseInt(next.getKey());
            Integer replicationFactor = next.getValue().get("replicationFactor").asInt();
            topicToReplicationFactor.put(id, replicationFactor);
        }
//        log.debug("trf:{}", topicToReplicationFactor);
    }

    public void serverThread(ServerThreadShared shared) throws IOException, AppException, InterruptedException {

        long t = System.nanoTime();
        ServerThreadState state = new ServerThreadState(Selector.open(), t, t, shared);
        Selector selector = state.selector;
        int count = 0;
        while(true) {
            state.now = System.nanoTime();
            processNewConns(state);
            int numSelects = selector.select(2000);
            if (numSelects == 0) {
//                log.debug("{} selected kets: {}", id, selector.keys().size());
                continue;
            }
            Set<SelectionKey> selectionKeys = selector.selectedKeys();
            Iterator<SelectionKey> iterator = selectionKeys.iterator();
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                Object o = key.attachment();
                if (o instanceof ProduceState) {
                    processProducer((ProduceState)o);
                } else if (o instanceof ReplicaConsumeState) {
//                    log.debug("{} 22222", id);
                    processReplicaConsumer((ReplicaConsumeState)o);
                }
                iterator.remove();
            }

        }
    }

    private void processProducer(ProduceState ps) throws IOException, AppException, InterruptedException {
        // start reading messages
        while(true) {
            processHeader(ps.msgState, ps.sc);

            if(ps.msgState.stateNow == HDR_JUST_FULL) {
                ps.topic = ps.msgState.header.getInt(0);

            } else if (ps.msgState.stateNow == HDR_INCOMPLETE) {
                if(ps.msgState.readNow == 0) {
                    return;
                }
            } else if (ps.msgState.stateNow == HDR_ALREADY_FULL){

                processBody(ps.msgState, ps.sc);
                if(ps.msgState.stateNow == INVALID) {
                    throw new AppException("Shouldnt be invalid");
                }
                if(ps.msgState.stateNow == MSG_INCOMPLETE && ps.msgState.readNow == 0) {
                    return;
                }
                if(ps.msgState.stateNow == MSG_COMPLETE) {

                    doReplicationAndAck(ps);
//                    log.debug("before reset: {}", ps.msgState);
                    resetMsgState(ps.msgState);
//                    log.debug("after reset: {}", ps.msgState);
//                    log.debug("");
//                    log.debug("");
//                    log.debug("");
                    return;
                }
            } else {
                throw new AppException("invalid return code");
            }
        }
    }

    private void resetMsgState(MessageState ms) throws AppException {
        ms.count = 0;
        ms.stateNow = INVALID;
        ms.readNow = 0;
        ms.read = 0;
        // release the memory
        ms.osMessage = null;
        ms.header.clear();
        if(ms.message.size() != 1) {
            throw new AppException("Something is wrong: linked list size should be 1");
        }
        ByteBuffer bb = ms.message.getFirst();
        ms.offset = 0;
        if(bb.limit() < bb.capacity()) {
            ms.offset = bb.limit();
            bb.position(bb.limit());
            bb.limit(bb.capacity());
        }
    }


    private void flipMsgState(MessageState ms) {
        ms.header.clear();
        ms.osMessage[1].position(ms.offset);
        for(int i=2; i<ms.count; i++) {
            ms.osMessage[i].position(0);
        }
    }

    private void processHeader(MessageState ms, SocketChannel sc) throws IOException {
        ms.stateNow = INVALID;
        ms.readNow = 0;
        if(ms.header.remaining() == 0) {
            ms.stateNow = HDR_ALREADY_FULL;
            return;
        }

        ms.readNow = sc.read(ms.header);
        if(ms.header.remaining() == 0) {
            ms.len = ms.header.getInt(4);
            ms.stateNow = HDR_JUST_FULL;
            ByteBuffer bb = ms.message.getFirst();
            if(ms.len < bb.remaining()) {
                bb.limit(bb.position()+ms.len);
            }
            return;
        }
        ms.stateNow = HDR_INCOMPLETE;
    }

    /**
     *
     * @return true if a message was processed
     * @throws IOException
     * @throws AppException
     */
    private void processBody(MessageState ms, SocketChannel sc) throws IOException, AppException {
        ms.stateNow = INVALID;
        ms.readNow = 0;

        // it is guaranteed that there will be at least one byte free in current,
        // so readNow can never be zero coz of no free space in user-space buffer.
        // it can be zero only coz data is unavailable in socket buffer.
        ms.readNow = sc.read(ms.current);
        ms.read += ms.readNow;
        if(ms.read == ms.len) {
            ms.stateNow = MSG_COMPLETE;
        } else {
            ms.stateNow = MSG_INCOMPLETE;
        }
        ByteBuffer current = ms.current;

        // enforce the free space guarantee
        if(current.remaining() == 0) {
            ms.current = ByteBuffer.allocate(BUF_SZ);
            ms.message.addLast(ms.current);
            if(ms.read + BUF_SZ > ms.len) {
                // if the buffer is greater than len, then restrict the current buffer size
                ms.current.limit(ms.len - ms.read);
            }
        }

        if(ms.stateNow == MSG_COMPLETE) {
            ms.osMessage = null;
            //TODO dont forget to keep the invariant that there is always a non full buffer in ps.message.
            // i.e., if the message happened to fill exactly the "current" bytebuffer, then allocate a new one.
            LinkedList<ByteBuffer> message = ms.message;
            ByteBuffer[] osMessage = ms.osMessageCache;
            if(message.size() + 1 > osMessage.length) { // the +1 for the header
//                log.warn("got a huge message. Allocating a new array");
                osMessage = new ByteBuffer[message.size()+1];
            }
            ms.osMessage = osMessage;
            int count = 0;
            osMessage[count++] = ms.header;

            // all but last
            while(message.size()>0) {
                osMessage[count++] = message.removeFirst();
            }

            // count is always at least one by the non full buffer invariant
            ByteBuffer bb = osMessage[count-1];

            if(bb.limit() < bb.capacity()) {
                message.add(bb);
            } else {
                message.add(ByteBuffer.allocate(BUF_SZ));
            }

            ms.count = count;
        }
    }

    private void doReplicationAndAck(ProduceState ps) throws IOException, AppException, InterruptedException {
//        log.debug("came to replicate!");
        int replicationFactor = topicToReplicationFactor.get(ps.topic);
        establishReplicaConnections(ps, replicationFactor);
        for(int i=0; i<replicationFactor; i++) {
            SocketChannel sc = ps.threadState.replicas.get(i);
            flipMsgState(ps.msgState);
//            if(!validateMessage(ps.msgState)) {
//                log.error("found an invalid message: {}", Arrays.toString(message));
//            }
            long writtenNow = 0;
            while(writtenNow!=ps.msgState.len+HEADER_LEN) {
                writtenNow += sc.write(ps.msgState.osMessage, 0, ps.msgState.count);
            }
//            log.debug("{} wrote to replicas: {}", i, written);
        }
//        log.debug("finished sending tp replicas");
        ByteBuffer bb = ps.threadState.replicaReply;
        for(int i=0; i<replicationFactor; i++) {
            SocketChannel sc = ps.threadState.replicas.get(i);
            bb.clear();
            bb.put(0, (byte)0);
            sc.read(bb);
            if(bb.get(0) != 1) {
                throw new AppException();
            }
//            log.debug("got ack from replica: {}", i);
        }

        bb.clear();
        bb.put(0, (byte)1);

        while(bb.remaining() > 0) {
            ps.sc.write(bb);
        }
        flipMsgState(ps.msgState);
//        StringBuilder s = new StringBuilder("Header bytes:");
//        s.append(ps.msgState.header.getInt(0)).append(", ").append(ps.msgState.header.getInt(4)).append(" Body bytes: ");
//        for(int i=1; i<ps.msgState.count; i++) {
//            ByteBuffer b = ps.msgState.osMessage[i];
//            for(int j=b.position(); j<b.limit(); j++) {
//                s.append(b.get(j)).append(", ");
//            }
//        }

//        log.debug(s.toString());
    }

    private boolean validateMessage(MessageState msgState) {
        ByteBuffer[] osMessage = msgState.osMessage;
        int count = 0;
        for(int i=1; i<msgState.count; i++) {
            for(int j=osMessage[i].position(); j<osMessage[i].limit(); j++) {
                if(message[count++]!=osMessage[i].get(j)) {
                    return false;
                }
            }
        }
        return true;
    }


    private void establishReplicaConnections(ProduceState ps, int replicationFactor) throws IOException, InterruptedException {
        if(ps.threadState.replicas.size() >= replicationFactor) {
            return;
        }
        ArrayNode replicas = (ArrayNode) config.get(id).get("replicas");
        for(int i=ps.threadState.replicas.size(); i<replicationFactor; i++) {
            String replicaId = replicas.get(i).asText();
            String ip = config.get(replicaId).get("ip").asText();
            int port = config.get(replicaId).get("replicaPort").asInt();
            SocketChannel sc = SocketChannel.open();
            sc.connect(new InetSocketAddress(ip, port));
            Thread.sleep(3);
            ps.threadState.replicas.add(sc);
        }
    }


    private void processReplicaConsumer(ReplicaConsumeState rs) throws IOException, AppException {
        // start reading messages
        while(true) {
            processHeader(rs.msgState, rs.sc);
            if(rs.msgState.stateNow == HDR_JUST_FULL) {
                rs.topic = rs.msgState.header.getInt(0);
            } else if (rs.msgState.stateNow == HDR_INCOMPLETE) {
                if(rs.msgState.readNow == 0) {
                    return;
                }
            } else if (rs.msgState.stateNow == HDR_ALREADY_FULL){
                processBody(rs.msgState, rs.sc);
//                if(id.equals("2")) log.debug("replica readNow:{} read:{}", rs.msgState.readNow, rs.msgState.read);
                if(rs.msgState.stateNow == INVALID) {
                    throw new AppException("Shouldnt be invalid");
                }
                if(rs.msgState.stateNow == MSG_INCOMPLETE && rs.msgState.readNow == 0) {
                    return;
                }
                if(rs.msgState.stateNow == MSG_COMPLETE) {
//                    log.debug("replica: got message!");
                    replicaAck(rs);
                    resetMsgState(rs.msgState);
                    return;
                }
            } else {
                throw new AppException("invalid return code");
            }
        }

    }

    private void replicaAck(ReplicaConsumeState rs) throws IOException {
//        if(!validateMessage(rs.msgState)) {
//            log.error("found an invalid message: {}", Arrays.toString(message));
//        }
        ByteBuffer replicaReply = rs.threadState.replicaReply;
        replicaReply.put(0, (byte)1);
        replicaReply.clear();
        while(replicaReply.remaining() > 0) {
            rs.sc.write(replicaReply);
        }
    }

    private void processNewConns(ServerThreadState state) throws IOException {
        long now = state.now;
        if(now - state.hundredMillis > 100_000_000) {
            List<SocketChannel> newConns = new ArrayList<>();
            state.shared.newProducerConnections.drainTo(newConns);
            for(SocketChannel sc : newConns) {
                ProduceState ps = new ProduceState( sc, state);
                sc.configureBlocking(false);
                SelectionKey key = sc.register(state.selector, SelectionKey.OP_READ, ps);
            }

            List<SocketChannel> newReplicaConns = new ArrayList<>();
            state.shared.newReplicaConnections.drainTo(newReplicaConns);
            for(SocketChannel sc : newReplicaConns) {
                ReplicaConsumeState rs = new ReplicaConsumeState( sc, state);
                sc.configureBlocking(false);
                SelectionKey key = sc.register(state.selector, SelectionKey.OP_READ, rs);
            }
            state.hundredMillis = now;
            if(newConns.size() > 0) {
                log.debug("processed {} conns", newConns.size());
            }
            if(newReplicaConns.size() > 0) {
            }

        }
    }

    public void producerListenerThread() throws IOException {
        int targetServerThread = 0;
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.bind(new InetSocketAddress(producePort));
        readyLatch.countDown();
        while(true) {
            SocketChannel socketChannel = ssc.accept();
            targetServerThread %= serverThreadShareds.size();
            serverThreadShareds.get(targetServerThread).newProducerConnections.add(socketChannel);
            targetServerThread++;
        }
    }

    public void replicaListenerThread() throws IOException {
        int targetServerThread = 0;
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.bind(new InetSocketAddress(replicaPort));
        readyLatch.countDown();
        while(true) {
            SocketChannel socketChannel = ssc.accept();
            serverThreadShareds.get(targetServerThread).newReplicaConnections.add(socketChannel);
            targetServerThread++;
            targetServerThread %= serverThreadShareds.size();
        }
    }

    static byte[] message;
    public static void main(String[] args) throws IOException, InterruptedException {
        UmbServer server = new UmbServer(args);
    }


    static void testMain(String[] args) {
        try {
            UmbServer server1 = new UmbServer("id 1".split(" "));
            UmbServer server2 = new UmbServer("id 2".split(" "));
            UmbServer server3 = new UmbServer("id 3".split(" "));

            Random random = new Random();
            Utils.LatencyTimer t = new Utils.LatencyTimer();
            int c = 0;
            int len = 32*1024;
            byte[] b = message = new byte[len];
            for (int i = 0; i < b.length; i++) {
                b[i] = (byte) random.nextInt();
            }

            int conn = 1;
            UmbClient2 client = new UmbClient2(conn, "config.json");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
