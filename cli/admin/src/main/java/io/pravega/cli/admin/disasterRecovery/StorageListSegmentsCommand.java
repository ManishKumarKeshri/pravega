package io.pravega.cli.admin.disasterRecovery;

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.server.host.StorageLoader;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.shared.NameUtils;
import io.pravega.shared.segment.SegmentToContainerMapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

@Slf4j
public class StorageListSegmentsCommand extends AdminCommand {

    private SegmentToContainerMapper segToConMapper;
    protected static final Logger LOGGER = Logger.getLogger(StorageListSegmentsCommand.class.getName());

    private static final List<String> HEADER = Arrays.asList("Sealed Status", "Length", "Segment Name");

    public StorageListSegmentsCommand(CommandArgs args) {
        super(args);
        segToConMapper = new SegmentToContainerMapper(getServiceConfig().getContainerCount());
    }

    @Override
    public void execute() throws Exception {
        LOGGER.setUseParentHandlers(false);
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());

        FileHandler fh = new FileHandler("ListSegmentsLog_" + timeStamp + ".log");
        fh.setLevel(Level.FINEST);
        DisasterRecoveryLogFormatter drFormatter = new DisasterRecoveryLogFormatter();
        fh.setFormatter(drFormatter);
        LOGGER.addHandler(fh);

        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(Level.INFO);
        LOGGER.setLevel(Level.ALL);
        ch.setFormatter(new SimpleFormatter() {
            private static final String format = "[%1$tF %1$tT] %3$s %n";

            @Override
            public synchronized String format(LogRecord lr) {
                return String.format(format,
                        new Date(lr.getMillis()),
                        lr.getLevel().getLocalizedName(),
                        lr.getMessage()
                );
            }
        });
        LOGGER.addHandler(ch);


        String filePath = System.getProperty("user.dir") + "/" + "Listed_segments_" + System.currentTimeMillis();
        if (getArgCount() >= 1) {
            filePath = getCommandArgs().getArgs().get(1);
            if(filePath.endsWith("/")) {
                filePath.substring(0, filePath.length()-1);
            }
        }

        ScheduledExecutorService scheduledExecutorService = ExecutorServiceHelpers.newScheduledThreadPool(1, "storageProcessor");

        // Load storage factory
        ServiceBuilder.ConfigSetupHelper configSetupHelper = new ServiceBuilder.ConfigSetupHelper(getServiceBuilderConfig());
        StorageLoader loader = new StorageLoader();
        StorageFactory storageFactory = loader.load(configSetupHelper, getServiceConfig().getStorageImplementation().toString(),
                getServiceConfig().getStorageLayout(), scheduledExecutorService);

        // Get the storage using the config.
        @Cleanup
        Storage storage = storageFactory.createStorageAdapter();
        LOGGER.log(Level.INFO, getServiceConfig().getStorageImplementation().toString() + " Storage initialized");

        int containerCount = segToConMapper.getTotalContainerCount();
        LOGGER.log(Level.INFO, "Container Count = " + containerCount);

        // Create a directory for storing files for each container.
        File dir = new File(filePath);
        if (!dir.exists()) {
            dir.mkdir();
        }

        // Create a file for each container.
        FileWriter[] csvWriters = new FileWriter[containerCount];
        for (int containerId=0; containerId < containerCount; containerId++) {
            File f = new File(filePath + "/" + "Container_" + containerId + ".csv");
            if(f.exists()){
                LOGGER.log(Level.FINE, "File already exists " + f.getAbsolutePath());
                if(!f.delete()) {
                    LOGGER.log(Level.SEVERE, "Failed to delete file " + f.getAbsolutePath());
                    return;
                }
            }
            if(!f.createNewFile()){
                LOGGER.log(Level.SEVERE, "Failed to create " + f.getAbsolutePath());
                return;
            }
            csvWriters[containerId] = new FileWriter(f.getName());
            LOGGER.log(Level.FINE, "Created file " + f.getPath(), Level.INFO);
            csvWriters[containerId].append(String.join(",", HEADER));
            csvWriters[containerId].append("\n");
        }

        // Gets total segments listed.
        int segmentsCount = 0;

        LOGGER.log(Level.INFO, "Writing segments' details to the files...");
        Iterator<SegmentProperties> segmentIterator = storage.listSegments();
        while(segmentIterator.hasNext()) {
            SegmentProperties currentSegment = segmentIterator.next();

            // skip recovery if the segment is an attribute segment.
            if (NameUtils.isAttributeSegment(currentSegment.getName())) {
                continue;
            }

            segmentsCount++;
            int containerId = segToConMapper.getContainerId(currentSegment.getName());
            LOGGER.log(Level.FINE, containerId + "\t" + currentSegment.isSealed() + "\t" + currentSegment.getLength() + "\t" +
                    currentSegment.getName());
            csvWriters[containerId].append(currentSegment.isSealed() + "," + currentSegment.getLength() + "," +
                    currentSegment.getName() + "\n");
        }

        LOGGER.log(Level.FINE, "Flushing data and closing the files...");
        for (int containerId=0; containerId < containerCount; containerId++) {
            csvWriters[containerId].flush();
            csvWriters[containerId].close();
        }
        LOGGER.log(Level.INFO, "All non-shadow segments' details have been written to the files.");
        LOGGER.log(Level.FINE, "Path to the directory of all files " + filePath);
        LOGGER.log(Level.INFO, "Total number of segments found: " + segmentsCount);
        LOGGER.log(Level.INFO, "Done listing the segments!");
    }

    public static CommandDescriptor descriptor() {
        final String component = "storage";
        return new CommandDescriptor(component, "list-segments", "lists segments from tier-2 and displays their name, length, sealed status",
                new ArgDescriptor("root", "mount path"));
    }
}
