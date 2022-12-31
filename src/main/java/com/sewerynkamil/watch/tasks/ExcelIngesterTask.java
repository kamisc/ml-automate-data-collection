package com.sewerynkamil.watch.tasks;

import com.sewerynkamil.model.SaleTransaction;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class ExcelIngesterTask extends IngesterTask {
    public ExcelIngesterTask(String sourceFilename, String targetFilename) {
        super(sourceFilename, targetFilename);
    }

    private void convertRowToTransaction(final Row row) {
        SaleTransaction transaction = new SaleTransaction.SaleTransactionBuilder()
                .uuid(row.getCell(0) != null ? row.getCell(0).getStringCellValue() : "")
                .timestamp(row.getCell(1) != null ? row.getCell(1).getStringCellValue() : "")
                .type(row.getCell(2) != null ? row.getCell(2).getStringCellValue() : "")
                .size(row.getCell(3) != null ? row.getCell(3).getStringCellValue() : "")
                .price(row.getCell(4) != null ? row.getCell(4).getStringCellValue() : "")
                .discount(row.getCell(5) != null ? row.getCell(5).getStringCellValue() : "")
                .offer(row.getCell(6) != null ? row.getCell(6).getStringCellValue() : "")
                .userId(row.getCell(7) != null ? (long) row.getCell(7).getNumericCellValue() : -1L)
                .country(SaleTransaction.Country.CANADA)
                .city("Montreal")
                .build();
        setTransaction(transaction);
    }

    @Override
    public void run() {
        try (Workbook workBook = new XSSFWorkbook(new FileInputStream(new File(getSourceFilename())))) {
            Sheet firstSheet = workBook.getSheetAt(0);
            for (Row row : firstSheet) {
                if (row.getRowNum() == 0) continue;
                convertRowToTransaction(row);
                storeTransactionInDataSink();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
