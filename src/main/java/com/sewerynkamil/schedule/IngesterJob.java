package com.sewerynkamil.schedule;

import com.sewerynkamil.schedule.tasks.*;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IngesterJob implements Job {
    private static final Logger LOG = LoggerFactory.getLogger(IngesterJob.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        ExecutorService executor = Executors.newCachedThreadPool();
        LOG.info("Running job {}", jobExecutionContext.getJobDetail().getKey());
        JobDataMap data = jobExecutionContext.getMergedJobDataMap();

        String sourceFolder = data.getString("sourceFolder");
        String targetFile = data.getString("targetFile");

        try {
            Files.newDirectoryStream(Paths.get(sourceFolder))
                    .forEach(filePath -> {
                        try {
                            String type = Files.probeContentType(filePath);
                            if (type != null) {

                                IngesterTaskWithDelete task = switch (type) {
                                    case "application/vnd.ms-excel", "text/csv" ->
                                            new CsvIngesterTaskWithDelete(filePath.toAbsolutePath().toString(), targetFile);
                                    case "text/xml", "application/xml" ->
                                            new XmlIngesterTaskWithDelete(filePath.toAbsolutePath().toString(), targetFile);
                                    case "text/json", "application/json" ->
                                            new JsonIngesterTaskWithDelete(filePath.toAbsolutePath().toString(), targetFile);
                                    case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ->
                                            new ExcelIngesterTaskWithDelete(filePath.toAbsolutePath().toString(), targetFile);
                                    default -> null;
                                };
                                if (task != null) executor.submit(task);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });

        } catch (IOException e) {
            throw new JobExecutionException(e);
        }
        Instant nextTrigger = jobExecutionContext.getNextFireTime().toInstant();
        LOG.info("Completed job {} - next run at {}", jobExecutionContext.getJobDetail().getKey(), nextTrigger);
    }
}
