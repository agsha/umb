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
import java.util.List;
import java.util.concurrent.*;

import static umb.common.Helper.IpPort;
public class UmbClient {
    private static final Logger log = LoggerFactory.getLogger(UmbClient.class);
    private final int threads;
    JsonNode config;

    public ArrayBlockingQueue<UmbMessage> q;
    ArrayList<IpPort> brokers = new ArrayList<>();
    public List<Thread> netThreads = new ArrayList<>();
    public static final int batchSize = 1024; // network batch size
    volatile boolean shutdown = false;

    public UmbClient(int threads) {
        config = Utils.readJsonFromClasspath("config.json", JsonNode.class);
        this.threads = threads;
        // keep the queue length small to avoid queueing delays
        q = new ArrayBlockingQueue<UmbMessage>(threads);
        ArrayNode brokersConfig = (ArrayNode)config.get("brokers");
        for(int i=0; i<brokersConfig.size(); i++) {
            String[] split = brokersConfig.get(i).asText().split(":");
            brokers.add(new IpPort(split[0], Integer.parseInt(split[1])));
        }

        startThreads();
    }

    private void startThreads() {
        CountDownLatch latch = new CountDownLatch(threads);

        for(int i=0; i<threads; i++) {
            long id = (long)i << 54;
            IpPort ipPort = brokers.get(threads % brokers.size());
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        network(ipPort, id, latch);
                    } catch (Exception e) {
                        log.error("something went wrong?", e);
                    }
                }
            });
            netThreads.add(t);
            t.start();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    public void network(IpPort ipPort, long threadId, CountDownLatch latch) throws InterruptedException, IOException {
        UmbSocketChannel socketChannel = UmbInjector.injector.getInstance(UmbSocketChannel.class);
        try {
            log.debug("connecting to {}:{}", ipPort.ip, ipPort.port);
            socketChannel.connect(new InetSocketAddress(ipPort.ip, ipPort.port));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        latch.countDown();

        long last = System.nanoTime();
        long msg = 0;
        ByteBuffer[] toSend = new ByteBuffer[2];
        ByteBuffer header = ByteBuffer.allocate(12);
        ByteBuffer ack = ByteBuffer.allocate(8);
        while(true) {
            UmbMessage message = q.poll(1, TimeUnit.SECONDS);
            if(message == null) {
                continue;
            }

            message.deQueueNanos = last;
            // 8 byte message id + 4 bytes message length
            header.clear();
            header.putLong((msg | threadId));
            header.putInt(message.payload.length);
            toSend[0] = header;
            toSend[1] = ByteBuffer.wrap(message.payload);
            socketChannel.write(toSend, 0, 1);

            ack.clear();
            while(ack.remaining() > 0) {
                socketChannel.read(ack);
            }
            if(ack.getLong(0) != (msg | threadId)) {
                throw new RuntimeException(String.format("ack mismatch! expected %d, got %d", (msg|threadId), ack.getLong(0)));
            }
            long now = System.nanoTime();
            message.nwNanos = now;
            message.future.latch.countDown();

            if(now - last > 1000_000_000) {
                // more than 1 sec
                if(shutdown) {
                    socketChannel.close();
                    return;
                }
            }
            last = now;

        }
    }


    public Future<Void> produce(int topic, byte[] ba) throws InterruptedException {
        UmbFuture f = new UmbFuture();
        UmbMessage message = new UmbMessage(ba, f);
        if(ba.length < batchSize / 2) {
            return smallMessage(ba);
        }
        message.queueAddNanos = System.nanoTime();
        q.put(message);
        return f;
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
        private UmbFuture future;
        long queueAddNanos;
        long deQueueNanos;
        long nwNanos;


        public UmbMessage(byte[] payload, UmbFuture f) {
            this.payload = payload;
            future = f;
        }
    }


    /**
     * Testing code
     */
    public static void main(String[] args) {
        try {
            test1();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * results of the test: max:612.48us 99.9%:36us 99.0%:20us 95.0%:11us 90.0%:10us 75.0%:10us 50.0%:9us 1.0%:9us  count:107000
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static void test1()  throws InterruptedException, ExecutionException  {
        class MyModule extends UmbModule {
            @Override
            protected void configure() {
                bind(UmbSocketChannel.class).to(MockUmbSocketChannelImpl.class);
            }
        }
        Injector injector = Guice.createInjector(Modules.override(new UmbModule()).with(new MyModule()));
        UmbInjector.injector = injector;
        System.out.println(UmbInjector.injector.getInstance(UmbSocketChannel.class));
        UmbClient c = new UmbClient(1);
        byte[] b = new byte[1024];
        Utils.LatencyTimer t = new Utils.LatencyTimer();
        int i=0;
        while(true) {
            Future<Void> future = c.produce(1, b);
            future.get();
            t.count();
        }

    }
}
