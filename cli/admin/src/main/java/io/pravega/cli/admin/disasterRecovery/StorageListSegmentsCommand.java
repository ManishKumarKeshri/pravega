package io.pravega.cli.admin.disasterRecovery;

import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.shared.NameUtils;
import io.pravega.shared.segment.SegmentToContainerMapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class StorageListSegmentsCommand extends DataRecoveryCommand {
    private final int containerCount;
    private final ScheduledExecutorService scheduledExecutorService = ExecutorServiceHelpers.newScheduledThreadPool(10,
            "listSegmentsProcessor");
    private final SegmentToContainerMapper segToConMapper;
    private static final List<String> HEADER = Arrays.asList("Sealed Status", "Length", "Segment Name");
    private final StorageFactory storageFactory;
    private static final int CONTAINER_EPOCH = 1;
    private String filePath;
    private final FileWriter[] csvWriters;
    public StorageListSegmentsCommand(CommandArgs args) {
        super(args);
        this.containerCount = getServiceConfig().getContainerCount();
        this.segToConMapper = new SegmentToContainerMapper(this.containerCount);
        this.storageFactory = getStorageFactory(scheduledExecutorService);
        this.csvWriters = new FileWriter[this.containerCount];;
    }

    private void createCSVFiles() throws Exception {
        // Create a csv file for each container to store segments for each.
        for (int containerId=0; containerId < this.containerCount; containerId++) {
            File f = new File(this.filePath + "/" + "Container_" + containerId + ".csv");
            if(f.exists()){
                log.debug("File '{}' already exists.", f.getAbsolutePath());
                if(!f.delete()) {
                    log.error("Failed to delete the file '{}'.", f.getAbsolutePath());
                    throw new Exception("Failed to delete the file " + f.getAbsolutePath());
                }
            }
            if(!f.createNewFile()){
                log.error("Failed to create file '{}'.", f.getAbsolutePath());
                throw new Exception("Failed to create file " + f.getAbsolutePath());
            }
            this.csvWriters[containerId] = new FileWriter(f.getName());
            log.trace("Created file '{}'", f.getAbsolutePath());
            this.csvWriters[containerId].append(String.join(",", HEADER));
            this.csvWriters[containerId].append("\n");
        }
    }

    @Override
    public void execute() throws Exception {
        filePath = setLogging(descriptor().getName());
        log.info("Container Count = {}", this.containerCount);
        // Get the storage using the config.
        @Cleanup
        Storage storage = this.storageFactory.createStorageAdapter();
        storage.initialize(CONTAINER_EPOCH);
        log.info("Loaded {} Storage.", getServiceConfig().getStorageImplementation().toString());

        // Gets total segments listed.
        int segmentsCount = 0;

        createCSVFiles();

        log.info("Writing segments' details to the csv files...");
        Iterator<SegmentProperties> segmentIterator = storage.listSegments();
        while(segmentIterator.hasNext()) {
            SegmentProperties currentSegment = segmentIterator.next();

            // skip recovery if the segment is an attribute segment.
            if (NameUtils.isAttributeSegment(currentSegment.getName())) {
                continue;
            }

            segmentsCount++;
            int containerId = segToConMapper.getContainerId(currentSegment.getName());
            log.debug(containerId + "\t" + currentSegment.isSealed() + "\t" + currentSegment.getLength() + "\t" +
                    currentSegment.getName());
            csvWriters[containerId].append(currentSegment.isSealed() + "," + currentSegment.getLength() + "," +
                    currentSegment.getName() + "\n");
        }

        log.debug("Closing all csv files...");
        for (int containerId=0; containerId < containerCount; containerId++) {
            csvWriters[containerId].flush();
            csvWriters[containerId].close();
        }

        log.info("All non-shadow segments' details have been written to the csv files.");
        log.debug("Path to the csv files: '{}'", filePath);
        log.info("Total number of segments found = {}", segmentsCount);
        log.info("Done listing the segments!");
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "list-segments", "lists segments from storage and displays their name, length, sealed status");
    }
}
