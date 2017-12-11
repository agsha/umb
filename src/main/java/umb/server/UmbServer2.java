package umb.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sha.Utils;
import umb.client.UmbClient2;
import umb.common.Helper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.CountDownLatch;


public class UmbServer2 {
    private static final Logger log = LoggerFactory.getLogger(UmbServer2.class);
    private JsonNode config;
    private String id;
    private int producePort;
    private int consumerPort;
    private int replicaPort;
    private int threads;
    private int msg;
    private final List<Thread> serverThreads;

    private final Thread producerListener;
    private final Thread replicaListener;
    HashMap<Integer, Integer> topicToReplicationFactor = new HashMap<>();
    CountDownLatch readyLatch = new CountDownLatch(2);

    int replicationFactor = 1;
    public UmbServer2(String[] args) {
        String conf = "config.json";
        int ii=0;
        while(ii < args.length) {
            if(args[ii].equals("conf")) {
                conf = args[ii+1];
            } else if(args[ii].equals("id")) {
                id = (args[ii+1]);
            } else if(args[ii].equals("replicationFactor")) {
                replicationFactor = Integer.parseInt(args[ii+1]);
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


    public void producerListenerThread() throws IOException {
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.bind(new InetSocketAddress(producePort));
        readyLatch.countDown();
        while(true) {
            SocketChannel socketChannel = ssc.accept();
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        serverThread(socketChannel);
                    } catch (IOException | InterruptedException | Helper.AppException e) {
                        e.printStackTrace();
                    }
                }
            });
            t.start();
        }
    }

    private void serverThread(SocketChannel socketChannel) throws IOException, InterruptedException, Helper.AppException {
        ArrayNode replicas = (ArrayNode) config.get(id).get("replicas");
        List<SocketChannel> replicaSockets = new ArrayList<>();
        for(int i=0; i<replicationFactor-1; i++) {
            String replicaId = replicas.get(i).asText();
            log.debug("replicaId: {}", replicaId);

            String ip = config.get(replicaId).get("ip").asText();
            int port = config.get(replicaId).get("replicaPort").asInt();
            SocketChannel sc = SocketChannel.open();
            sc.connect(new InetSocketAddress(ip, port));
            Thread.sleep(3);
            replicaSockets.add(sc);
        }

        ByteBuffer b = ByteBuffer.allocate(msg);
        ByteBuffer ack = ByteBuffer.allocate(8);
        ack.put(0, (byte)1);
        while(true) {
            socketChannel.read(b);
            if(b.remaining() == 0) {
                // got a message
                // do replication and ack back
                for(int i=0; i<replicationFactor-1; i++) {
                    b.clear();
                    while(b.remaining() > 0) {
                        replicaSockets.get(i).write(b);
                    }
                }

                for(int i=0; i<replicationFactor-1; i++) {
                    ack.clear();
                    while(ack.remaining() > 0) {
                        replicaSockets.get(i).read(ack);
                    }
                    if(ack.get(0)!=1) {
                        throw new Helper.AppException("incorrect ack");
                    }
                }

                ack.clear();
                while(ack.remaining() >0) {
                    socketChannel.write(ack);
                }
                b.clear();
            }
        }
    }

    public void replicaListenerThread() throws IOException {
        int targetServerThread = 0;
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.bind(new InetSocketAddress(replicaPort));
        readyLatch.countDown();
        while(true) {
            SocketChannel socketChannel = ssc.accept();
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        replicaThread(socketChannel);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            t.start();

        }
    }

    private void replicaThread(SocketChannel socketChannel) throws IOException {
        ByteBuffer b = ByteBuffer.allocate(msg);
        ByteBuffer ack = ByteBuffer.allocate(8);
        ack.put(0, (byte)1); // a positive ack
        while(true) {
            socketChannel.read(b);
            if(b.remaining() == 0) {
                while(ack.remaining() > 0) {
                    socketChannel.write(ack);
                }
                ack.clear();
                b.clear();
            }
        }
    }

    static byte[] message;
    public static void main(String[] args) throws IOException, InterruptedException {
        UmbServer2 server = new UmbServer2(args);
    }


    static void testMain(String[] args) {
        try {
            UmbServer2 server1 = new UmbServer2("id 1".split(" "));
            UmbServer2 server2 = new UmbServer2("id 2".split(" "));
            UmbServer2 server3 = new UmbServer2("id 3".split(" "));

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
