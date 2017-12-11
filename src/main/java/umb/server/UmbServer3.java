package umb.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sha.Utils;
import umb.client.UmbClient2;
import umb.common.Helper3;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static umb.common.Helper3.*;

public class UmbServer3 {
    private static final Logger log = LoggerFactory.getLogger(UmbServer3.class);
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
    private int msg;

    public UmbServer3(String[] args) {
        String conf = "config.json";
        int ii=0;
        while(ii < args.length) {
            if(args[ii].equals("conf")) {
                conf = args[ii+1];
            } else if(args[ii].equals("id")) {
                id = (args[ii+1]);
            } else if(args[ii].equals("msg")) {
                msg = Integer.parseInt(args[ii+1]);
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
        Helper3.BUF_SZ = msg;

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
        MessageState state = ps.msgState;
        ByteBuffer current = state.current;
        while(true) {
            int readNow = ps.sc.read(current);
            if(readNow == 0) {
                break;
            }
        }
        if(current.remaining() == 0) {
//            log.debug("finished getting message, now replicating");
            doReplicationAndAck(ps);
            current.clear();
        }
    }



    private void doReplicationAndAck(ProduceState ps) throws IOException, AppException, InterruptedException {
        MessageState state = ps.msgState;
        ByteBuffer current = state.current;

//        log.debug("came to replicate!");
        int replicationFactor = topicToReplicationFactor.get(ps.topic);
        establishReplicaConnections(ps, replicationFactor);
        for(int i=0; i<replicationFactor; i++) {
            SocketChannel sc = ps.threadState.replicas.get(i);
            current.clear();
//            if(!validateMessage(ps.msgState)) {
//                log.error("found an invalid message: {}", Arrays.toString(message));
//            }
            while(current.remaining() > 0) {
//                log.debug("{} came to write: remaining:{}", i, current.remaining());
                sc.write(current);
            }
//            log.debug("{} wrote to replicas", i);
        }
//        log.debug("finished sending tp replicas");
        ByteBuffer bb = state.replicaReply;
        for(int i=0; i<replicationFactor; i++) {
            SocketChannel sc = ps.threadState.replicas.get(i);
            bb.clear();
            bb.put(0, (byte)0);
            while(bb.remaining() > 0) {
                sc.read(bb);
            }
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
//        log.debug("finished writing ack back to client");
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

//    private boolean validateMessage(MessageState msgState) {
//        ByteBuffer[] osMessage = msgState.osMessage;
//        int count = 0;
//        for(int i=1; i<msgState.count; i++) {
//            for(int j=osMessage[i].position(); j<osMessage[i].limit(); j++) {
//                if(message[count++]!=osMessage[i].get(j)) {
//                    return false;
//                }
//            }
//        }
//        return true;
//    }


    private void establishReplicaConnections(ProduceState ps, int replicationFactor) throws IOException, InterruptedException {
        if(ps.threadState.replicas.size() >= replicationFactor) {
            return;
        }
        ArrayNode replicas = (ArrayNode) config.get(id).get("replicas");
        for(int i=ps.threadState.replicas.size(); i<replicationFactor; i++) {
            log.debug("replica {} myid:{} ", i, id);
            log.debug("ArrayNode replicas: {}", replicas);
            String replicaId = replicas.get(i).asText();
            log.debug("replicaId: {}", replicaId);

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
        MessageState state = rs.msgState;
        ByteBuffer current = state.current;

        while(true) {
            int readNow = rs.sc.read(current);
            if(readNow == 0) break;
        }
        if(current.remaining() == 0) {
            ByteBuffer replicaReplyRecv = state.replicaReply;
            replicaReplyRecv.clear();
            replicaReplyRecv.put(0, (byte)1);
            while(replicaReplyRecv.remaining() > 0) {
                rs.sc.write(replicaReplyRecv);
            }
            current.clear();
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
        UmbServer3 server = new UmbServer3(args);
    }


    static void testMain(String[] args) {
        try {
            UmbServer3 server1 = new UmbServer3("id 1".split(" "));
            UmbServer3 server2 = new UmbServer3("id 2".split(" "));
            UmbServer3 server3 = new UmbServer3("id 3".split(" "));

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
