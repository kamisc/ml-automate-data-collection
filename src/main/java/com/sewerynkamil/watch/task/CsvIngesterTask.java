package com.sewerynkamil.watch.task;

import com.sewerynkamil.model.SaleTransaction;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

public class CsvIngesterTask extends IngesterTask {
    private CSVRecord record;

    public CsvIngesterTask(String sourceFilename, String targetFilename) {
        super(sourceFilename, targetFilename);
    }

    private void recordToSale() {
        SaleTransaction transaction = new SaleTransaction.SaleTransactionBuilder()
                .uuid(record.get("txid"))
                .timestamp(record.get("txts"))
                .type(record.get("coffee"))
                .size(record.get("size"))
                .price(record.get("price"))
                .discount(record.get("discount"))
                .offer(record.get("offer"))
                .userId(Long.parseLong(record.get("userid")))
                .country(SaleTransaction.Country.UK)
                .city("London")
                .build();

        setTransaction(transaction);
    }

    @Override
    public void run() {
        try (Reader in = new FileReader(getSourceFilename())) {
            Iterable<CSVRecord> records = CSVFormat.Builder.create(CSVFormat.RFC4180)
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .build()
                    .parse(in);
            for (CSVRecord rec : records) {
                record = rec;
                recordToSale();
                storeTransactionInDataSink();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
