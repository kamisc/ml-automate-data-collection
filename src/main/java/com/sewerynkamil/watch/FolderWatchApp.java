package com.sewerynkamil.watch;

import com.sewerynkamil.watch.task.CsvIngesterTask;
import com.sewerynkamil.watch.task.IngesterTask;
import com.sewerynkamil.watch.task.XmlIngesterTask;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FolderWatchApp {
    public static void main(String[] args) {
        ExecutorService executor = Executors.newCachedThreadPool();
        String sourceFolder = "example-data";
        String targetFile = "datasink/sales-global.dat";

        try {
            Path sourcePath = Paths.get(sourceFolder);
            Path targetPath = Paths.get(targetFile);

            try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
                WatchKey key = sourcePath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
                System.out.println("WatchKey = " + key);
                while ((key = watchService.take()) != null) {
                    for (WatchEvent<?> event : key.pollEvents()) {
                        if (event.context() instanceof Path) {
                            Path filePath = sourcePath.resolve((Path) event.context());
                            System.out.println("FilePath: " + filePath);
                            String type = Files.probeContentType(filePath);
                            if (type != null) {
                                IngesterTask task = switch (type) {
                                    case "application/vnd.ms-excel", "text/csv" -> new CsvIngesterTask(filePath.toAbsolutePath().toString(), targetPath.toAbsolutePath().toString());
                                    case "text/xml", "application/xml" -> new XmlIngesterTask(filePath.toAbsolutePath().toString(), targetPath.toAbsolutePath().toString());
                                    default -> null;
                                };
                                if (task != null) executor.submit(task);
                                System.out.println("Event kind: " + event.kind() + ". File affected: " + event.context() + ". Type: " + type);
                            }
                        }
                    }
                    key.reset();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
            System.out.println("Interrupted");
            System.exit(0);
        }
    }
}
