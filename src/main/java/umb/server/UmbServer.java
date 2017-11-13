package umb.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sha.Utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;

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

    public UmbServer(String id) {
        this.id = id;
        config = Utils.readJsonFromClasspath("server_config.json", JsonNode.class);
        JsonNode myConfig = config.get("id");
        producePort = myConfig.get("producePort").asInt();
        consumerPort = myConfig.get("consumerPort").asInt();
        replicaPort = myConfig.get("replicaPort").asInt();
        threads = myConfig.get("numServerThreads").asInt();
        topicMetadata(config);
        serverThreads = new ArrayList<>();
        serverThreadShareds = new ArrayList<>();
        for(int i=0; i<threads; i++) {
            ServerThreadShared s = new ServerThreadShared();
            serverThreadShareds.add(s);
            Thread t = new Thread(() -> {
                try {
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
    }

    private void topicMetadata(JsonNode config) {
        ObjectNode on = (ObjectNode)config.get("topics");
        Iterator<Map.Entry<String, JsonNode>> fields = on.fields();
        while(fields.hasNext()) {
            Map.Entry<String, JsonNode> next = fields.next();
            Integer id = Integer.getInteger(next.getKey());
            Integer replicationFactor = next.getValue().get("replicationFactor").asInt();
            topicToReplicationFactor.put(id, replicationFactor);
        }
    }

    public void serverThread(ServerThreadShared shared) throws IOException, AppException {

        long t = System.nanoTime();
        ServerThreadState state = new ServerThreadState(Selector.open(), t, t, shared);
        Selector selector = state.selector;
        while(true) {
            state.now = System.nanoTime();
            processNewConns(state);
            int numSelects = selector.select(100);
            if (numSelects == 0) {
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
                    processReplicaConsumer(state, (ReplicaConsumeState)o);
                }
                iterator.remove();
            }

        }
    }

    private void processProducer(ProduceState ps) throws IOException, AppException {
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
                    resetMsgState(ps.msgState);
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
        // release the memory
        ms.osMessage = null;
        ms.header.clear();
        if(ms.message.size() != 1) {
            throw new AppException("Something is wrong: linked list size should be 1");
        }
        ByteBuffer bb = ms.message.getFirst();
        bb.limit(bb.capacity());
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
        if(current.remaining() == 0 && ms.stateNow == MSG_INCOMPLETE) {
            ms.current = ByteBuffer.allocate(1024);
            ms.message.addLast(ms.current);
            if(ms.read + 1024 > ms.len) {
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
            if(message.size() > osMessage.length) {
                log.warn("got a huge message. Allocating a new array");
                osMessage = new ByteBuffer[message.size()];
            }
            ms.osMessage = osMessage;
            int count = 0;
            osMessage[count++] = ms.header;
            int sofar = 0;
            while(sofar < ms.len) {
                ByteBuffer bb = message.getFirst();
                osMessage[count++] = bb;
                sofar += bb.remaining();
                if(bb.limit() == bb.capacity()) {
                    message.removeFirst();
                }
            }
            if(message.size() == 0) {
                message.add(ByteBuffer.allocate(1024));
            }
            ms.count = count;
        }
    }

    private void doReplicationAndAck(ProduceState ps) throws IOException, AppException {
        int replicationFactor = topicToReplicationFactor.get(ps.topic);
        establishReplicaConnections(ps, replicationFactor);
        for(int i=0; i<replicationFactor; i++) {
            SocketChannel sc = ps.threadState.replicas.get(i);
            sc.configureBlocking(false);
            sc.write(ps.msgState.osMessage, 0, ps.msgState.count);
        }
        ByteBuffer bb = ps.threadState.replicaReply;
        for(int i=0; i<replicationFactor; i++) {
            SocketChannel sc = ps.threadState.replicas.get(i);
            sc.configureBlocking(true);
            bb.clear();
            sc.read(bb);
            if(bb.get() != 1) {
                throw new AppException();
            }
        }
        ps.sc.configureBlocking(true);
        bb.clear();
        ps.sc.write(bb);
        ps.sc.configureBlocking(false);
    }

    private void establishReplicaConnections(ProduceState ps, int replicationFactor) throws IOException {
        if(ps.threadState.replicas.size() >= replicationFactor) {
            return;
        }
        ArrayNode replicas = (ArrayNode) config.get(id).get("replicas");
        for(int i=ps.threadState.replicas.size(); i<replicationFactor; i++) {
            String replicaId = replicas.get(i).asText();
            String ip = config.get(replicaId).get("ip").asText();
            int port = config.get(replicaId).get("port").asInt();
            SocketChannel sc = SocketChannel.open();
            sc.connect(new InetSocketAddress(ip, port));
            ps.threadState.replicas.add(sc);
        }
    }


    private void processReplicaConsumer(ServerThreadState state, ReplicaConsumeState rcs) {
        throw new RuntimeException("not implemented");
        //TODO
    }


    private void processNewConns(ServerThreadState state) throws IOException {
        long now = state.now;
        if(now - state.hundredMillis > 100_000_000) {
            List<SocketChannel> newConns = new ArrayList<>();
            state.shared.newProducerConnections.drainTo(newConns);
            for(SocketChannel sc : newConns) {
                ProduceState ps = new ProduceState(0, 0, sc, state);
                sc.configureBlocking(false);
                sc.register(state.selector, SelectionKey.OP_READ, ps);
            }
            state.hundredMillis = now;
        }
    }

    public void producerListenerThread() throws IOException {
        int targetServerThread = 0;
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.bind(new InetSocketAddress("127.0.0.1", producePort));
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
        ssc.bind(new InetSocketAddress("127.0.0.1", replicaPort));
        while(true) {
            SocketChannel socketChannel = ssc.accept();
            serverThreadShareds.get(targetServerThread).newReplicaConnections.add(socketChannel);
            targetServerThread++;
            targetServerThread %= serverThreadShareds.size();
        }
    }

}
