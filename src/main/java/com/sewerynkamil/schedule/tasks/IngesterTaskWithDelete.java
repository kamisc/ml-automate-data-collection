package com.sewerynkamil.schedule.tasks;

import com.sewerynkamil.model.SaleTransaction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public abstract class IngesterTaskWithDelete implements Runnable {
    private final String sourceFilename;
    private final String targetFilename;
    private SaleTransaction transaction;

    public IngesterTaskWithDelete(String sourceFilename, String targetFilename) {
        this.sourceFilename = sourceFilename;
        this.targetFilename = targetFilename;
    }

    public String getSourceFilename() {
        return sourceFilename;
    }

    public String getTargetFilename() {
        return targetFilename;
    }

    protected SaleTransaction getTransaction() {
        return transaction;
    }

    protected void setTransaction(SaleTransaction transaction) {
        this.transaction = transaction;
    }

    protected void storeTransactionInDataSink() throws IOException {
        System.out.println("[" + Thread.currentThread().getName() + "] Storing " + getTransaction().toString());
        String transactionString = getTransaction().toString() + "\n";
        Files.write(
                Paths.get(getTargetFilename()),
                transactionString.getBytes(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND
        );
    }

    @Override
    public void run() {
        runInternal();
        try {
            Files.delete(Path.of(getSourceFilename()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected abstract void runInternal();
}
