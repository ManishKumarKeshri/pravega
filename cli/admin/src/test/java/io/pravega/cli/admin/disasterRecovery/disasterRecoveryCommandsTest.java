package io.pravega.cli.admin.disasterRecovery;

import io.pravega.cli.admin.AbstractAdminCommandTest;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;

public class disasterRecoveryCommandsTest extends AbstractAdminCommandTest {
    private static final Duration TIMEOUT = Duration.ofMillis(30 * 1000);
    private static final int NUM_EVENTS = 300;
    private static final String EVENT = "12345";

//    @Before
//    public void setUp() throws IOException {
//        STATE.set(new AdminCommandState());
//
//    }

    @Test
    public void testListSegmentsCommand() throws Exception {
        int numSegments = 10;
        String streamName = "testListSegmentsCommand";
        SETUP_UTILS.createTestStream(streamName, numSegments);
        writeData(streamName);
        SETUP_UTILS.getController().close();
        SETUP_UTILS.getClientFactory().close();
        flushEverything(TIMEOUT);
        String commandResult = TestUtils.executeCommand("storage list-segments", STATE.get());
        Assert.assertTrue(commandResult.contains("Total number of segments found: " + numSegments));
        Assert.assertNotNull(StorageListSegmentsCommand.descriptor());
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
}
