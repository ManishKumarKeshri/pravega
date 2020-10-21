package io.pravega.cli.admin.disasterRecovery;

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.segmentstore.server.host.StorageLoader;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.storage.StorageFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public abstract class DataRecoveryCommand extends AdminCommand {
    protected final static String COMPONENT = "storage";

    DataRecoveryCommand(CommandArgs args) {
        super(args);
    }

    StorageFactory getStorageFactory(ScheduledExecutorService executorService) {
        ServiceBuilder.ConfigSetupHelper configSetupHelper = new ServiceBuilder.ConfigSetupHelper(getServiceBuilderConfig());
        StorageLoader loader = new StorageLoader();
        return loader.load(configSetupHelper, getServiceConfig().getStorageImplementation().toString(),
                getServiceConfig().getStorageLayout(), executorService);
    }

    String setLogging(String commandName) throws Exception {
        String fileSuffix = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        String fileName = commandName + fileSuffix + ".log";

        String filePath = System.getProperty("user.dir") + "/" + commandName + "_" + fileSuffix;
        log.info("pwd = {}", System.getProperty("user.dir"));
        if (getArgCount() >= 1) {
            filePath = getCommandArgs().getArgs().get(0);
            if(filePath.endsWith("/")) {
                filePath = filePath.substring(0, filePath.length()-1);
            }
        }

        // Create a directory for storing files.
        File dir = new File(filePath);
        if (!dir.exists()) {
            dir.mkdir();
        }

        File f = new File(filePath + "/" + fileName);
        if(f.exists()){
            log.debug("Logging File '{}' already exists.", f.getAbsolutePath());
            if(!f.delete()) {
                log.error("Failed to delete the file '{}'.", f.getAbsolutePath());
                throw new Exception("Failed to delete the file " + f.getAbsolutePath());
            }
        }
        if(!f.createNewFile()){
            log.error("Failed to create file '{}'.", f.getAbsolutePath());
            throw new Exception("Failed to create file " + f.getAbsolutePath());
        }

        log.info("Logs are written to file '{}'", filePath + "/" + fileName);
        System.setProperty("logFilename", filePath + "/" + fileName);
        return filePath;
    }
}
