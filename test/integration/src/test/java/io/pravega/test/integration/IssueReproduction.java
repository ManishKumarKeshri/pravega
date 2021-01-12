package io.pravega.test.integration;

import com.sun.jersey.core.util.LazyVal;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class IssueReproduction {

    private static final ArrayList<String> STREAM_NAMES = new ArrayList<>(Arrays.asList("testStream1", "testStream2", "testStream3"));
    // num of streams can be increased up to 3
    private static final int NUM_STREAMS = 1;
    private static final int NUM_WRITERS_PER_STREAM = 3;
    private static final int NUM_READERS_PER_STREAM = 1;
    private static final int NUM_EVENTS_PER_BATCH = 5;
    private static final int EVENT_SIZE = 600;
    private static final int NUM_TRIES = 2;
    private static final Random RANDOM = new Random(567);
    private AtomicLong eventReadCount;
    private AtomicLong eventData;
    private ServiceBuilder serviceBuilder;
    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    private ScheduledExecutorService writerPool;
    private ScheduledExecutorService readerPool;
    private byte[] writeData;

    @Before
    public void setup() throws Exception {

        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.getAvailableListenPort();
        final int containerCount = 4;

        // 1. Start ZK
        this.zkTestServer = new TestingServerStarter().start();

        // 2. Start Pravega SegmentStore service.
        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        this.server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor());
        this.server.startListening();

        // 3. Start Pravega Controller service
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();
        this.writerPool = ExecutorServiceHelpers.newScheduledThreadPool(NUM_WRITERS_PER_STREAM * NUM_STREAMS, "WriterPool");
        this.readerPool = ExecutorServiceHelpers.newScheduledThreadPool(NUM_READERS_PER_STREAM * NUM_STREAMS, "ReaderPool");
    }

    @After
    public void tearDown() throws Exception {

        if (this.controllerWrapper != null) {
            this.controllerWrapper.close();
            this.controllerWrapper = null;
        }
        if (this.controller != null) {
            this.controller.close();
            this.controller = null;
        }
        if (this.server != null) {
            this.server.close();
            this.server = null;
        }
        if (this.serviceBuilder != null) {
            this.serviceBuilder.close();
            this.serviceBuilder = null;
        }
        if (this.zkTestServer != null) {
            this.zkTestServer.close();
            this.zkTestServer = null;
        }

        if (this.writerPool != null) {
            ExecutorServiceHelpers.shutdown(this.writerPool);
            this.writerPool = null;
        }

        if (this.readerPool != null) {
            ExecutorServiceHelpers.shutdown(this.readerPool);
            this.readerPool = null;
        }
    }

    @Test(timeout = 2000000)
    public void readWriteTest() throws InterruptedException, ExecutionException {

        String scope = "testScope";
        ArrayList<String> readerGroupNames = new ArrayList<>(Arrays.asList("testReaderGroup1", "testReaderGroup2", "testReaderGroup3"));
        // 1 segment fixed
        ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();

        eventData = new AtomicLong(0);
        eventReadCount = new AtomicLong(0); // used by readers to maintain a count of events.
        ClientConfig clientConfig = ClientConfig.builder().build();
        try (ConnectionPool cp = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
             StreamManager streamManager = new StreamManagerImpl(controller, cp)) {
            //create a scope
            Boolean createScopeStatus = streamManager.createScope(scope);
            log.info("Create scope status {}", createScopeStatus);

            Boolean createStreamStatus;
            //create streams
            for (int i = 0; i < NUM_STREAMS; i++) {
                createStreamStatus = streamManager.createStream(scope, STREAM_NAMES.get(i), config);
                log.info("Create stream status {}", createStreamStatus);
            }
        }

        try (ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
             ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);
             ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory)) {

            // create writers
            val writersMap = createWritersByStream(clientFactory);

            //create reader groups
            for (int i = 0; i < NUM_STREAMS; i++) {
                log.info("Creating Reader group : {}", readerGroupNames.get(i));

                readerGroupManager.createReaderGroup(readerGroupNames.get(i), ReaderGroupConfig.builder().stream(Stream.of(scope, STREAM_NAMES.get(i))).build());
                log.info("Reader group name {} ", readerGroupManager.getReaderGroup(readerGroupNames.get(i)).getGroupName());
                log.info("Reader group scope {}", readerGroupManager.getReaderGroup(readerGroupNames.get(i)).getScope());
            }

            // write first batch of events
            for (int j = 0; j < NUM_TRIES; j++) {
                val eventBatch1 = writeEventsBatch(NUM_EVENTS_PER_BATCH, writersMap);

                Futures.allOf(eventBatch1).get();
                sleep(3000);

                // Events are written in the order
                // batch 1 - batch2 - wait
                // batch 1 - batch2 - wait
                // batch 1 - batch2 - wait

                //write second batch of events
                val eventBatch2 = writeEventsBatch(NUM_EVENTS_PER_BATCH, writersMap);

                Futures.allOf(eventBatch2).get();

                //create readers
                log.info("Creating readers");
                List<CompletableFuture<Void>> readerList = new ArrayList<>();
                //start reading events
                for (int i = 0; i < NUM_STREAMS; i++) {
                    log.info("Starting reader for stream {}", STREAM_NAMES.get(i));
                    String readerName = "reader" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
                    readerList.add(startNewReader(readerName + i + j, clientFactory, readerGroupNames.get(i),
                            2 * NUM_EVENTS_PER_BATCH * (j + 1) * NUM_WRITERS_PER_STREAM * NUM_STREAMS, eventReadCount));
                }

                //wait for readers completion
                Futures.allOf(readerList).get();

                log.info("All writers have stopped. Setting Stop_Read_Flag. Event Written Count:{}, Event Read " +
                        "Count: {}", eventData.get(), eventReadCount.get());
                assertEquals(2 * NUM_EVENTS_PER_BATCH * (j + 1) * NUM_WRITERS_PER_STREAM * NUM_STREAMS, eventReadCount.get());

                sleep(3000);
            }

            ExecutorServiceHelpers.shutdown(writerPool);
            // close writers
            for (val writers : writersMap.values()) {
                for (val writer : writers) {
                    writer.close();
                }
            }

            ExecutorServiceHelpers.shutdown(readerPool);

            //delete readergroup
            for (int i = 0; i < NUM_STREAMS; i++) {
                String readerGroupName = readerGroupNames.get(i);
                log.info("Deleting readergroup {}", readerGroupName);
                readerGroupManager.deleteReaderGroup(readerGroupName);
            }
        }

        //seal the stream
        for (int i = 0; i < NUM_STREAMS; i++) {
            String stream = STREAM_NAMES.get(i);
            CompletableFuture<Boolean> sealStreamStatus = controller.sealStream(scope, stream);
            log.info("Sealing stream {}", stream);
            assertTrue(sealStreamStatus.get());
            //delete the stream
            CompletableFuture<Boolean> deleteStreamStatus = controller.deleteStream(scope, stream);
            log.info("Deleting stream {}", stream);
            assertTrue(deleteStreamStatus.get());
        }
        //delete the  scope
        CompletableFuture<Boolean> deleteScopeStatus = controller.deleteScope(scope);
        log.info("Deleting scope {}", scope);
        assertTrue(deleteScopeStatus.get());
        log.info("Read write test succeeds");
    }

    private List<CompletableFuture<Void>> writeEventsBatch(long num_events, Map<String, List<EventStreamWriter<byte[]>>> writersMap) {
        List<CompletableFuture<Void>> writerList = new ArrayList<>();
        for (int i = 0; i < NUM_STREAMS; i++) {
            String streamName = STREAM_NAMES.get(i);
            log.info("Writing on stream {}", streamName);
            for (int j = 0; j < NUM_WRITERS_PER_STREAM; j++) {
                writerList.add(write(eventData, writersMap.get(streamName).get(j), num_events));
            }
        }
        return writerList;
    }

    private Map<String, List<EventStreamWriter<byte[]>>> createWritersByStream(final EventStreamClientFactory clientFactory) {
        Map<String, List<EventStreamWriter<byte[]>>> writersMap = new HashMap<>();
        for (int i = 0; i < NUM_STREAMS; i++) {
            String streamName = STREAM_NAMES.get(i);
            log.info("Creating {} writers for stream {}", NUM_WRITERS_PER_STREAM, streamName);
            for (int j = 0; j < NUM_WRITERS_PER_STREAM; j++) {
                log.info("Starting writer{}", j);
                EventStreamWriter<byte[]> writer = clientFactory.createEventWriter(streamName,
                        new JavaSerializer<byte[]>(), EventWriterConfig.builder().build());
                if (writersMap.get(streamName) == null) {
                    writersMap.put(streamName, new ArrayList<>());
                }
                writersMap.get(streamName).add(writer);
            }
        }
        return writersMap;
    }

    private CompletableFuture<Void> write(final AtomicLong data, EventStreamWriter<byte[]> writer, long num_events) {
        return CompletableFuture.runAsync(() -> {
            for (int i = 0; i < num_events; i++) {
                writeData = populate(EVENT_SIZE);
                log.info("Data written: {}", writeData);
                // data.incrementAndGet();
                writer.writeEvent("fixed", writeData);
                writer.flush();
            }
            log.info("Closing writer {}", writer);
        }, writerPool);
    }

    private void populate(byte[] data) {
        RANDOM.nextBytes(data);
    }

    private byte[] populate(int size) {
        byte[] bytes = new byte[size];
        populate(bytes);
        return bytes;
    }

    private CompletableFuture<Void> startNewReader(final String id, final EventStreamClientFactory clientFactory, final String
            readerGroupName, int writeCount, final AtomicLong readCount) {
        return CompletableFuture.runAsync(() -> {
            @Cleanup
            final EventStreamReader<byte[]> reader = clientFactory.createReader(id,
                    readerGroupName,
                    new JavaSerializer<byte[]>(),
                    ReaderConfig.builder().build());
            byte[] byteEvent;
            while(true) {
                byteEvent = reader.readNextEvent(SECONDS.toMillis(100)).getEvent();
                if (byteEvent != null) {
                    log.info("Data read: {}", byteEvent);
                    //update if event read is not null.
                    readCount.incrementAndGet();
                } else {
                    break;
                }
            }
            log.info("Closing reader {}", reader);
            reader.close();
        }, readerPool);
    }
}
