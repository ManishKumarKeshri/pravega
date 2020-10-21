package io.pravega.cli.admin.disasterRecovery;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.storage.filesystem.FileSystemStorageFactory;
import io.pravega.test.integration.utils.SetupUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class Tier1RecoveryCommandTest extends BookKeeperClusterTestCase {
    @Getter
    private ScheduledExecutorService executor;
    private static final Duration TIMEOUT = Duration.ofMillis(30 * 1000);
    private static final int NUM_EVENTS = 300;
    private static final String EVENT = "12345";
    private File baseDir = null;
    private FileSystemStorageConfig adapterConfig;
    private StorageFactory storageFactory = null;

    // Setup utility.
    protected static final SetupUtils SETUP_UTILS = new SetupUtils();
    protected static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    @Rule
    public final Timeout globalTimeout = new Timeout(600, TimeUnit.SECONDS);
    private static final Duration READ_TIMEOUT = Duration.ofMillis(1000);

    public Tier1RecoveryCommandTest() {
        super(1);
    }

    @Before
    public void setUp() throws Exception {
        this.executor = ExecutorServiceHelpers.newScheduledThreadPool(2, "storage pool");
        this.baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
        this.adapterConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .with(FileSystemStorageConfig.REPLACE_ENABLED, true)
                .build();

        this.storageFactory = new FileSystemStorageFactory(adapterConfig, this.executor);

        SETUP_UTILS.startAllServices(this.storageFactory);

        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controller.rest.uri", SETUP_UTILS.getControllerRestUri().toString());
        pravegaProperties.setProperty("cli.controller.grpc.uri", SETUP_UTILS.getControllerUri().toString());
        pravegaProperties.setProperty("pravegaservice.zk.connect.uri", SETUP_UTILS.getZkTestServer().getConnectString());
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.storage.impl.name", "FILESYSTEM");
        pravegaProperties.setProperty("pravegaservice.storage.layout", "ROLLING_STORAGE");
        pravegaProperties.setProperty("filesystem.root", this.baseDir.getAbsolutePath());
        STATE.get().getConfigBuilder().include(pravegaProperties);
    }

    @Test
    public void testDataRecoveryCommand() throws Exception {
        int numSegments = 10;
        String streamName = "testListSegmentsCommand";
        SETUP_UTILS.createTestStream(streamName, numSegments);
        writeData(streamName);
        SETUP_UTILS.getController().close();
        SETUP_UTILS.getClientFactory().close();
        flushEverything(TIMEOUT);
        SETUP_UTILS.stopAllServices();
        Properties bkProperties = new Properties();
        log.info("zk connect uri = {}", zkUtil.getZooKeeperConnectString());
        bkProperties.setProperty("pravegaservice.zk.connect.uri", zkUtil.getZooKeeperConnectString());
        bkProperties.setProperty("bookkeeper.ledger.path", "/ledgers");
        bkProperties.setProperty("bookkeeper.zk.metadata.path", "ledgers");
        bkProperties.setProperty("pravegaservice.clusterName", "");
        STATE.get().getConfigBuilder().include(bkProperties);
        String commandResult = TestUtils.executeCommand("storage Tier1-recovery ./build", STATE.get());
        SETUP_UTILS.startSegmentStoreAndController(this.storageFactory, zkUtil.getZooKeeperConnectString());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controller.rest.uri", SETUP_UTILS.getControllerRestUri().toString());
        pravegaProperties.setProperty("cli.controller.grpc.uri", SETUP_UTILS.getControllerUri().toString());
        STATE.get().getConfigBuilder().include(pravegaProperties);
        readAllEvents(streamName);
        Assert.assertNotNull(StorageListSegmentsCommand.descriptor());
    }

    @After
    public void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
        STATE.get().close();
        FileHelpers.deleteFileOrDirectory(baseDir);
        baseDir = null;
    }

    private void flushEverything(Duration timeout) throws ContainerNotFoundException {
        ServiceBuilder.ComponentSetup componentSetup = new ServiceBuilder.ComponentSetup(SETUP_UTILS.getServiceBuilder());
        int containerCount = componentSetup.getContainerRegistry().getContainerCount();
        for (int containerId = 0; containerId < containerCount; containerId++) {
            componentSetup.getContainerRegistry().getContainer(containerId).flushToStorage(timeout).join();
        }
    }

    private void writeData(String streamName) {
        EventStreamWriter<String> writer = SETUP_UTILS.getClientFactory().createEventWriter(streamName,
                new UTF8StringSerializer(),
                EventWriterConfig.builder().build());
        for (int i = 0; i < NUM_EVENTS;) {
            writer.writeEvent("", EVENT).join();
            i++;
        }
        writer.flush();
        writer.close();
    }

    // Reads the required number of events from the stream.
    private void readAllEvents(String streamName) {
        String readerGroupName = "RG";
        String readerName = "R";
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(SETUP_UTILS.getControllerUri()).build());
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SETUP_UTILS.getScope(), SETUP_UTILS.getController(), connectionFactory);
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SETUP_UTILS.getScope(), SETUP_UTILS.getController(), clientFactory);
        readerGroupManager.createReaderGroup(readerGroupName,
                ReaderGroupConfig
                        .builder()
                        .stream(Stream.of(SETUP_UTILS.getScope(), streamName))
                        .build());

        EventStreamReader<String> reader = clientFactory.createReader(readerName,
                readerGroupName,
                new UTF8StringSerializer(),
                ReaderConfig.builder().build());

        for (int q = 0; q < NUM_EVENTS;) {
            String eventRead = reader.readNextEvent(READ_TIMEOUT.toMillis()).getEvent();
            //log.info("EventRead = {}", eventRead);
            Assert.assertEquals("Event written and read back don't match", EVENT, eventRead);
            q++;
        }
        reader.close();
    }
}
