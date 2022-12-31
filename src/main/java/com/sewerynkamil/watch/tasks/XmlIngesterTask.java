package com.sewerynkamil.watch.tasks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.sewerynkamil.model.SaleTransaction;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class XmlIngesterTask extends IngesterTask {
    public XmlIngesterTask(String sourceFilename, String targetFilename) {
        super(sourceFilename, targetFilename);
    }

    @Override
    public void run() {
        ObjectMapper objectMapper = new XmlMapper();
        try {
            List<SaleTransaction> saleTransactionList = objectMapper.readValue(new File(getSourceFilename()), new TypeReference<List<SaleTransaction>>() {});
            for (SaleTransaction transaction : saleTransactionList) {
                transaction.setCountry(SaleTransaction.Country.ITALY);
                transaction.setCity("Turin");
                setTransaction(transaction);
                storeTransactionInDataSink();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
