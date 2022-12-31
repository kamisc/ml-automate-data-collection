package com.sewerynkamil.watch.tasks;

import com.sewerynkamil.model.SaleTransaction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public abstract class IngesterTask implements Runnable {
    private final String sourceFilename;
    private final String targetFilename;
    private SaleTransaction transaction;

    protected IngesterTask(String sourceFilename, String targetFilename) {
        this.sourceFilename = sourceFilename;
        this.targetFilename = targetFilename;
    }

    public String getSourceFilename() {
        return sourceFilename;
    }

    public String getTargetFilename() {
        return targetFilename;
    }

    protected void setTransaction(SaleTransaction transaction) {
        this.transaction = transaction;
    }

    protected SaleTransaction getTransaction() {
        return transaction;
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
}
