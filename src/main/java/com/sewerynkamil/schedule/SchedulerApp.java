package com.sewerynkamil.schedule;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.quartz.DailyTimeIntervalScheduleBuilder.dailyTimeIntervalSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class SchedulerApp {
    private static final Logger LOG = LoggerFactory.getLogger(SchedulerApp.class);

    public static void main(String[] args) {
        LOG.info("Starting data sales ingestion");

        Properties props = System.getProperties();
        int dayRunEveryMins;
        int nightRunEveryMins;
        String sourceFolder;
        String targetFolder;

        try {
            dayRunEveryMins = Integer.parseInt(props.getProperty("dayRunMins", "5"));
            nightRunEveryMins = Integer.parseInt(props.getProperty("nightRunMins", "180"));
            sourceFolder = props.getProperty("sourceFolder", "example-data");
            targetFolder = props.getProperty("targetFolder", "datasink");
        } catch (NumberFormatException e) {
            LOG.error("Configuration error: dayRunMins or nightRunMins is not a valid number");
            System.exit(1);
            return;
        }
        LOG.info("Running with dayRunMins {}, nightRunMins {} from sourceFolder {} to targetFolder {}", dayRunEveryMins, nightRunEveryMins, sourceFolder, targetFolder);

        SchedulerFactory sf = new StdSchedulerFactory();
        Scheduler scheduler = null;

        try {
            scheduler = sf.getScheduler();

            JobDetail job = newJob(IngesterJob.class)
                    .withIdentity("ingest-job", "sales-transactions")
                    .usingJobData("sourceFolder", sourceFolder)
                    .usingJobData("targetFile", targetFolder + "/global-sales.dat")
                    .storeDurably()
                    .build();

            scheduler.addJob(job, false);

            LOG.info("Scheduling job for {} to run every {} minutes during the day", targetFolder, dayRunEveryMins);
            Trigger dayTrigger = newTrigger()
                    .withIdentity("ingest-trigger-day", "sales-transactions")
                    .startNow()
                    .withSchedule(dailyTimeIntervalSchedule()
                            .onEveryDay()
                            .withIntervalInMinutes(dayRunEveryMins)
                            .startingDailyAt(TimeOfDay.hourAndMinuteOfDay(7, 30))
                            .endingDailyAt(TimeOfDay.hourAndMinuteOfDay(22, 30)))
                    .forJob(job)
                    .build();

            LOG.info("Scheduling job for {} to run every {} minutes at night", targetFolder, nightRunEveryMins);
            Trigger nightTrigger = newTrigger()
                    .withIdentity("ingest-trigger-night", "sales-transactions")
                    .startNow()
                    .withSchedule(dailyTimeIntervalSchedule()
                            .onMondayThroughFriday()
                            .withIntervalInMinutes(nightRunEveryMins)
                            .startingDailyAt(TimeOfDay.hourAndMinuteOfDay(0, 0))
                            .endingDailyAt(TimeOfDay.hourAndMinuteOfDay(6, 0)))
                    .forJob(job)
                    .build();

            scheduler.scheduleJob(dayTrigger);
            scheduler.scheduleJob(nightTrigger);

            scheduler.start();

            LOG.info("Scheduled job for {} every {} and {} minutes", targetFolder, dayTrigger, nightTrigger);

            final Scheduler schedulerAtSchutdown = scheduler;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    LOG.info("Shutting down data sales ingestion");
                    schedulerAtSchutdown.shutdown(true);
                } catch (SchedulerException e) {
                    LOG.error("Problem shutting the scheduler down", e);
                }
            }));
        } catch (SchedulerException e) {
            LOG.error("Problem with scheduler", e);
            if (scheduler != null) {
                try {
                    scheduler.shutdown();
                } catch (SchedulerException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
}
