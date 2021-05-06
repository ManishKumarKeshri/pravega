package io.pravega.cli.admin.segmentstore;

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FlushToStorageCommand extends SegmentStoreCommand {
    private static final int REQUEST_TIMEOUT_SECONDS = 10;

    /**
     * Creates a new instance of the FlushToStorageCommand.
     *
     * @param args The arguments for the command.
     */

    public FlushToStorageCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws ExecutionException, InterruptedException, TimeoutException {
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        SegmentHelper segmentHelper = instantiateSegmentHelper(zkClient);
        CompletableFuture<WireCommands.SegmentRead> reply = segmentHelper.flushToStorage() readSegment(
                offset, length, new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), "");
        WireCommands.SegmentRead segmentRead = reply.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        output("ReadSegment: %s", segmentRead.toString());
        output("SegmentRead content: %s", segmentRead.getData().toString(StandardCharsets.UTF_8));
    }

    public static AdminCommand.CommandDescriptor descriptor() {
        return new AdminCommand.CommandDescriptor(COMPONENT, "read-segment", "Read a range from a given Segment.",
                new AdminCommand.ArgDescriptor("qualified-segment-name", "Fully qualified name of the Segment to get info from (e.g., scope/stream/0.#epoch.0)."),
                new AdminCommand.ArgDescriptor("offset", "Starting point of the read request within the target Segment."),
                new AdminCommand.ArgDescriptor("length", "Number of bytes to read."),
                new AdminCommand.ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}
