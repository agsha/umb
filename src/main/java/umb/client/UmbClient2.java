package umb.client;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sha.Utils;
import umb.common.UmbSocketChannel;
import umb.guice.UmbInjector;
import umb.guice.UmbModule;
import umb.testing.MockUmbSocketChannelImpl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static umb.common.Helper.IpPort;

public class UmbClient2 {
    private static final Logger log = LoggerFactory.getLogger(UmbClient2.class);
    private final int connections;
    JsonNode config;

    ArrayList<IpPort> brokers = new ArrayList<>();
    ArrayBlockingQueue<UmbSocketChannel> socketChannels ;
    public static final int batchSize = 1024; // network batch size
    volatile boolean shutdown = false;
    AtomicInteger messageId = new AtomicInteger();
    public UmbClient2(int connections, String conf) throws InterruptedException {
        config = Utils.readJsonFromClasspath(conf, JsonNode.class);
        this.connections = connections;
        // keep the queue length small to avoid queueing delays
        ArrayNode brokersConfig = (ArrayNode)config.get("brokers");
        socketChannels = new ArrayBlockingQueue<>(connections);
        for(int i=0; i<brokersConfig.size(); i++) {
            String[] split = brokersConfig.get(i).asText().split(":");
            brokers.add(new IpPort(split[0], Integer.parseInt(split[1])));
        }
        startConnections();
    }

    private void startConnections() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(connections);

        for(int i = 0; i< connections; i++) {
            long id = (long)i << 54;
            IpPort ipPort = brokers.get(connections % brokers.size());
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        doConnect(ipPort, id, latch);
                    } catch (Exception e) {
                        log.error("something went wrong?", e);
                    }
                }
            });
            t.start();
            Thread.sleep(5);
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private void doConnect(IpPort ipPort, long id, CountDownLatch latch) throws InterruptedException {
        UmbSocketChannel socketChannel = UmbInjector.injector.getInstance(UmbSocketChannel.class);
        try {
            log.debug("connecting to {}:{}", ipPort.ip, ipPort.port);
            socketChannel.connect(new InetSocketAddress(ipPort.ip, ipPort.port));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        socketChannels.put(socketChannel);
        latch.countDown();
    }


    public void produce(int topic, byte[] ba) throws InterruptedException, IOException {
        UmbSocketChannel socketChannel = socketChannels.take();
        long id = messageId.incrementAndGet();
        UmbFuture f = new UmbFuture();
        UmbMessage message = new UmbMessage(ba, f);
//        if(ba.length < batchSize / 2) {
//            smallMessage(ba);
//        }

        ByteBuffer header = ByteBuffer.allocate(8);
        ByteBuffer ack = ByteBuffer.allocate(1);

        // 8 byte message id + 4 bytes message length
        header.clear();
        header.putInt(0, 1);
        header.putInt(4, message.payload.length);
        ByteBuffer[] toSend = new ByteBuffer[2];
        toSend[0] = header;
        toSend[1] = ByteBuffer.wrap(message.payload);
//        log.debug("before write");
        //TODO, put the remaining count here.
        socketChannel.write(toSend, 0, 2);

        ack.clear();
        while(ack.remaining() > 0) {
            socketChannel.read(ack);
        }
//        log.debug("after ack");

        if(ack.get(0) != 1) {
            throw new RuntimeException(String.format("ack mismatch! expected %d, got %d", id, ack.get(0)));
        }
        socketChannels.put(socketChannel);
    }

    private Future<Void> smallMessage(byte[] ba) {
        throw new RuntimeException("unimplemented");
    }


    public static class UmbFuture implements Future<Void> {
        CountDownLatch latch = new CountDownLatch(1);
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return latch.getCount() == 0;
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            latch.await();
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            latch.await(timeout, unit);
            return null;
        }
    }


    public static class UmbMessage {
        byte[] payload;


        public UmbMessage(byte[] payload, UmbFuture f) {
            this.payload = payload;
        }
    }


    /**
     * Testing code
     */
    public static void main(String[] args) {
        realMain(args);
//        test1(args);
    }

    static void realMain(String[] args) {
        int conn = 1;
        String conf = "config.json";
        int ii = 0;
        while(ii < args.length) {
            if(args[ii].equals("conn")) {
                conn = Integer.parseInt(args[ii+1]);
            } else if(args[ii].equals("conf")) {
                conf = args[ii+1];
            }
            ii+=2;
        }

        try {
            UmbClient2 client = new UmbClient2(conn, conf);
            Random random = new Random();
            Utils.LatencyTimer t = new Utils.LatencyTimer();
            int c = 0;
            int len = 1*1024;
            byte[]b =  new byte[len];
            for(int j=0; j<b.length; j++) {
                b[j] = (byte)random.nextInt();
            }

            for (int i = 0; i < conn; i++) {
                Thread tt = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            try {
                                client.produce(1, b);
                            } catch (InterruptedException | IOException e) {
                                e.printStackTrace();
                            }
                            t.count();
                        }
                    }
                });
                tt.start();
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            Thread.sleep(60_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    /**
     * results of the test: max:612.48us 99.9%:36us 99.0%:20us 95.0%:11us 90.0%:10us 75.0%:10us 50.0%:9us 1.0%:9us  count:107000
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static void test1(String[] args) {
        try {
            int conn = 1;
            String conf = "config.json";
            int ii = 0;
            while(ii < args.length) {
                if(args[ii].equals("conn")) {
                    conn = Integer.parseInt(args[ii+1]);
                } else if(args[ii].equals("conf")) {
                    conf = args[ii+1];
                }
                ii+=2;
            }
            conn = 1;

            class MyModule extends UmbModule {
                @Override
                protected void configure() {
                    bind(UmbSocketChannel.class).to(MockUmbSocketChannelImpl.class);
                }
            }
            Injector injector = Guice.createInjector(Modules.override(new UmbModule()).with(new MyModule()));
            UmbInjector.injector = injector;
            System.out.println(UmbInjector.injector.getInstance(UmbSocketChannel.class));

            try {
                UmbClient2 client = new UmbClient2(conn, conf);
                Random random = new Random();
                Utils.LatencyTimer t = new Utils.LatencyTimer();
                int c = 0;
                int len = 1*1024;
                byte[]b =  new byte[len];
                for(int j=0; j<b.length; j++) {
                    b[j] = (byte)random.nextInt();
                }

                for (int i = 0; i < conn; i++) {
                    Thread tt = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            while (true) {
                                try {
                                    client.produce(1, b);
                                } catch (InterruptedException | IOException e) {
                                    e.printStackTrace();
                                }
                                t.count();
                            }
                        }
                    });
                    tt.start();
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
