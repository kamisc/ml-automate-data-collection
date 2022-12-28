package com.sewerynkamil.watch.task;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.sewerynkamil.model.SaleTransaction;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class JsonIngesterTask extends IngesterTask {
    public JsonIngesterTask(String sourceFilename, String targetFilename) {
        super(sourceFilename, targetFilename);
    }

    @Override
    public void run() {
        ObjectMapper objectMapper = new JsonMapper();

        try {
            List<SaleTransaction> saleTransactionList = objectMapper.readValue(new File(getSourceFilename()), new TypeReference<List<SaleTransaction>>() {});
            for (SaleTransaction transaction : saleTransactionList) {
                transaction.setCountry(SaleTransaction.Country.JAPAN);
                transaction.setCity("Tokyo");
                setTransaction(transaction);
                storeTransactionInDataSink();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
