package io.pravega.cli.admin.disasterRecovery;

import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerFactory;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.attributes.AttributeIndexConfig;
import io.pravega.segmentstore.server.attributes.AttributeIndexFactory;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndexFactoryImpl;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.containers.ContainerRecoveryUtils;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainer;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.logs.DurableLogFactory;
import io.pravega.segmentstore.server.reading.ContainerReadIndexFactory;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.server.tables.ContainerTableExtensionImpl;
import io.pravega.segmentstore.server.writer.StorageWriterFactory;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.shared.NameUtils;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Tier1RecoveryCommand extends DataRecoveryCommand {
    private final int containerCount;
    private final StorageFactory storageFactory;
    private static final int CONTAINER_EPOCH = 1;
    private String filePath;
    private static final Duration TIMEOUT = Duration.ofMillis(100 * 1000);
    private static final DurableLogConfig DURABLE_LOG_CONFIG = DurableLogConfig
            .builder()
            .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 1)
            .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 10)
            .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10L * 1024 * 1024)
            .build();
    private static final ContainerConfig DEFAULT_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, 10 * 60)
            .build();
    // Configurations for DebugSegmentContainer
    private static final ContainerConfig CONTAINER_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, (int) DEFAULT_CONFIG.getSegmentMetadataExpiration().getSeconds())
            .with(ContainerConfig.MAX_ACTIVE_SEGMENT_COUNT, 100)
            .build();

    ScheduledExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(100, "recoveryProcessor");

    @SneakyThrows
    public Tier1RecoveryCommand(CommandArgs args) {
        super(args);
        this.containerCount = getServiceConfig().getContainerCount();
        this.storageFactory = getStorageFactory(executorService);
    }

    public void execute() throws Exception {
        setLogging(descriptor().getName());
        log.info("Container Count = {}", this.containerCount);

        @Cleanup
        Storage storage = this.storageFactory.createStorageAdapter();
        storage.initialize(CONTAINER_EPOCH);
        log.info("Loaded {} Storage.", getServiceConfig().getStorageImplementation().toString());

        val serviceConfig = getServiceConfig();
        val bkConfig = getCommandArgs().getState().getConfigBuilder()
                .include(BookKeeperConfig.builder().with(BookKeeperConfig.ZK_ADDRESS, serviceConfig.getZkURL()))
                .build().getConfig(BookKeeperConfig::builder);
        val zkClient = createZKClient();
        val factory = new BookKeeperLogFactory(bkConfig, zkClient, getCommandArgs().getState().getExecutor());
        try {
            factory.initialize();
        } catch (DurableDataLogException ex) {
            zkClient.close();
            throw ex;
        }

        log.info("Starting recovery...");
        // Map<Integer, String> backUpMetadataSegments = getBackUpMetadataSegments(storage, this.containerCount, executorService);

        for (int containerId = 0; containerId < containerCount; containerId++) {
            ContainerRecoveryUtils.deleteMetadataAndAttributeSegments(storage, containerId).join();
        }

        @Cleanup
        ContainerContext context = createContainerContext(executorService);
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = getContainers(context, this.containerCount, factory,
                this.storageFactory);

        log.info("Recovering all segments...");
        ContainerRecoveryUtils.recoverAllSegments(storage, debugStreamSegmentContainerMap, executorService);
        log.info("All segments recovered.");

        // Update core attributes from the backUp Metadata segments
        // ContainerRecoveryUtils.updateCoreAttributes(backUpMetadataSegments, debugStreamSegmentContainerMap, executorService);

        // Waits for metadata segments to be flushed to LTS and then stops the debug segment containers
        stopDebugSegmentContainersPostFlush(debugStreamSegmentContainerMap);
        log.info("Segments have been recovered.");
        log.info("Recovery Done!");
    }

    // Closes the debug segment container instances in the given map after waiting for the metadata segment to be flushed to
    // the given storage.
    private void stopDebugSegmentContainersPostFlush(Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap)
            throws Exception {
        for (val debugSegmentContainer : debugStreamSegmentContainerMap.values()) {
            log.debug("Waiting for metadata segment of container {} to be flushed to the Long-Term storage.", debugSegmentContainer.getId());
            debugSegmentContainer.flushToStorage(TIMEOUT).join();
            Services.stopAsync(debugSegmentContainer, executorService).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info("Stopping debug segment container {}.", debugSegmentContainer.getId());
            debugSegmentContainer.close();
        }
    }

    // Back up and delete container metadata segment and attributes index segment corresponding to each container Ids from the long term storage
    private Map<Integer, String> getBackUpMetadataSegments(Storage storage, int containerCount, ScheduledExecutorService executorService)
            throws Exception {
        String fileSuffix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        Map<Integer, String> backUpMetadataSegments = new HashMap<>();

        val futures = new ArrayList<CompletableFuture<Void>>();

        for (int containerId = 0; containerId < containerCount; containerId++) {
            String backUpMetadataSegment = NameUtils.getMetadataSegmentName(containerId) + fileSuffix;
            String backUpAttributeSegment = NameUtils.getAttributeSegmentName(backUpMetadataSegment);

            int finalContainerId = containerId;
            futures.add(Futures.exceptionallyExpecting(
                    ContainerRecoveryUtils.backUpMetadataAndAttributeSegments(storage, containerId,
                            backUpMetadataSegment, backUpAttributeSegment, executorService)
                            .thenAccept(x -> ContainerRecoveryUtils.deleteMetadataAndAttributeSegments(storage, finalContainerId)
                                    .thenAccept(z -> backUpMetadataSegments.put(finalContainerId, backUpMetadataSegment))
                            ), ex -> Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException, null));
        }
        Futures.allOf(futures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return backUpMetadataSegments;
    }

    // Creates debug segment container instances, puts them in a map and returns it.
    private Map<Integer, DebugStreamSegmentContainer> getContainers(ContainerContext context, int containerCount,
                                                                                  BookKeeperLogFactory dataLogFactory,
                                                                                  StorageFactory storageFactory) throws Exception {
        // Start a debug segment container corresponding to the given container Id and put it in the Hashmap with the Id.
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = new HashMap<>();
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(DURABLE_LOG_CONFIG, dataLogFactory, executorService);

        // Create a debug segment container instances using a
        for (int containerId = 0; containerId < containerCount; containerId++) {
            MetadataCleanupContainer debugStreamSegmentContainer = new MetadataCleanupContainer(containerId, CONTAINER_CONFIG, localDurableLogFactory,
                    context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, storageFactory,
                    context.getDefaultExtensions(), executorService);

            Services.startAsync(debugStreamSegmentContainer, executorService).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            debugStreamSegmentContainerMap.put(containerId, debugStreamSegmentContainer);
        }
        return debugStreamSegmentContainerMap;
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "Tier1-recovery", "reconcile segments from container");
    }

    public static class MetadataCleanupContainer extends DebugStreamSegmentContainer {
        private final ScheduledExecutorService executor;

        public MetadataCleanupContainer(int streamSegmentContainerId, ContainerConfig config, OperationLogFactory durableLogFactory,
                                        ReadIndexFactory readIndexFactory, AttributeIndexFactory attributeIndexFactory,
                                        WriterFactory writerFactory, StorageFactory storageFactory,
                                        SegmentContainerFactory.CreateExtensions createExtensions, ScheduledExecutorService executor) {
            super(streamSegmentContainerId, config, durableLogFactory, readIndexFactory, attributeIndexFactory, writerFactory,
                    storageFactory, createExtensions, executor);
            this.executor = executor;
        }
    }

    public static ContainerContext createContainerContext(ScheduledExecutorService scheduledExecutorService) {
        return new ContainerContext(scheduledExecutorService);
    }


    public static class ContainerContext implements AutoCloseable {

        private static final ReadIndexConfig DEFAULT_READ_INDEX_CONFIG = ReadIndexConfig.builder().with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024).build();

        private static final AttributeIndexConfig DEFAULT_ATTRIBUTE_INDEX_CONFIG = AttributeIndexConfig
                .builder()
                .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, 2 * 1024)
                .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 1000)
                .build();

        private static final WriterConfig DEFAULT_WRITER_CONFIG = WriterConfig
                .builder()
                .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1)
                .with(WriterConfig.FLUSH_ATTRIBUTES_THRESHOLD, 3)
                .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 25L)
                .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
                .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L)
                .build();

        public DurableDataLogFactory dataLogFactory;
        public final ReadIndexFactory readIndexFactory;
        public final AttributeIndexFactory attributeIndexFactory;
        public final WriterFactory writerFactory;
        public final CacheStorage cacheStorage;
        public final CacheManager cacheManager;

        ContainerContext(ScheduledExecutorService scheduledExecutorService) {
            this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE / 5);
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, scheduledExecutorService);
            this.readIndexFactory = new ContainerReadIndexFactory(DEFAULT_READ_INDEX_CONFIG, this.cacheManager, scheduledExecutorService);
            this.attributeIndexFactory = new ContainerAttributeIndexFactoryImpl(DEFAULT_ATTRIBUTE_INDEX_CONFIG, this.cacheManager, scheduledExecutorService);
            this.writerFactory = new StorageWriterFactory(DEFAULT_WRITER_CONFIG, scheduledExecutorService);
        }

        public SegmentContainerFactory.CreateExtensions getDefaultExtensions() {
            return (c, e) -> Collections.singletonMap(ContainerTableExtension.class, createTableExtension(c, e));
        }

        private ContainerTableExtension createTableExtension(SegmentContainer c, ScheduledExecutorService e) {
            return new ContainerTableExtensionImpl(c, this.cacheManager, e);
        }

        @Override
        public void close() {
            this.readIndexFactory.close();
            this.cacheManager.close();
            this.cacheStorage.close();
        }
    }
}
