/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.disasterRecovery;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.storage.filesystem.FileSystemStorageFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.integration.utils.SetupUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests list segments command.
 */
public class StorageListSegmentsCommandTest {
    private static final Duration TIMEOUT = Duration.ofMillis(30 * 1000);
    private static final int NUM_EVENTS = 30;
    private static final String EVENT = "12345";

    // Setup utility.
    private static final SetupUtils SETUP_UTILS = new SetupUtils();
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(2, "list-segments pool");
    @Rule
    private final Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    private File baseDir = null;
    private FileSystemStorageConfig adapterConfig;
    private StorageFactory storageFactory = null;
    private File tempDir = null;

    @Before
    public void setUp() throws Exception {
        this.baseDir = Files.createTempDirectory("testListSegments").toFile().getAbsoluteFile();
        this.tempDir = Files.createTempDirectory("listSegments").toFile().getAbsoluteFile();
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

    @After
    public void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
        STATE.get().close();
        FileHelpers.deleteFileOrDirectory(baseDir);
        FileHelpers.deleteFileOrDirectory(tempDir);
    }

    @Test
    public void testListSegmentsCommand() throws Exception {
        String streamName = "testListSegmentsCommand";
        SETUP_UTILS.createTestStream(streamName, 100);
        writeData(streamName);
        SETUP_UTILS.getController().close();
        SETUP_UTILS.getClientFactory().close();
        flushEverything(TIMEOUT);
        TestUtils.executeCommand("storage list-segments " + this.tempDir.getAbsolutePath(), STATE.get());
        Assert.assertTrue(new File(tempDir.getAbsolutePath(), "Container_0.csv").exists());
        Path path = Paths.get(tempDir.getAbsolutePath() + "/Container_0.csv");
        long lines = Files.lines(path).count();
        AssertExtensions.assertGreaterThan("There should be at least one segment.", lines, 1);
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
