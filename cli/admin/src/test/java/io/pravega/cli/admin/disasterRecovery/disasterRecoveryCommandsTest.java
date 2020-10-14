package io.pravega.cli.admin.disasterRecovery;

import io.pravega.cli.admin.AbstractAdminCommandTest;
import io.pravega.cli.admin.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class disasterRecoveryCommandsTest extends AbstractAdminCommandTest {
    @Test
    public void testListSegmentsCommand() throws Exception {
        int numSegments = 10;
        SETUP_UTILS.createTestStream("testListSegmentsCommand", numSegments);
        String commandResult = TestUtils.executeCommand("storage list-segments", STATE.get());
        // Check that both the new scope and the system one exist.
        Assert.assertTrue(commandResult.contains("Total number of segments found: " + numSegments));
        Assert.assertNotNull(StorageListSegmentsCommand.descriptor());
    }
}
