package com.tradr_data_pipeline.processor;

//import com.tradr_data_pipeline.model.StockRecord;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//public class StockDataProcessorOld {

//    private static final Path DATA_DIR = Paths.get("data");
//    private static final Path HISTORICAL_DATA_DIR = DATA_DIR.resolve("historical_data");
//    private static final Path PROCESSED_DATA_DIR = DATA_DIR.resolve("processed_data");
//
//    public static void main(String[] args) {
//        System.out.println("Starting stock data processing...");
//        try {
//            Files.createDirectories(PROCESSED_DATA_DIR);
//
//            // Get all CSV files from the historical_data directory
//            List<Path> csvFiles = Files.walk(HISTORICAL_DATA_DIR)
//                    .filter(Files::isRegularFile)
//                    .filter(path -> path.toString().endsWith(".csv"))
//                    .collect(Collectors.toList());
//
//            if (csvFiles.isEmpty()) {
//                System.err.println("No historical data files found in " + HISTORICAL_DATA_DIR);
//                return;
//            }
//
//            // Process each CSV file
//            for (Path filePath : csvFiles) {
//                String ticker = filePath.getFileName().toString().replace(".csv", "");
//                System.out.println("Processing data for " + ticker);
//                processTickerData(filePath, ticker);
//            }
//
//            System.out.println("Data processing complete. Check the 'processed_data' directory.");
//        } catch (IOException e) {
//            System.err.println("An error occurred during data processing: " + e.getMessage());
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * Reads a CSV file, calculates the target variable, and saves the result.
//     *
//     * @param filePath The path to the raw CSV file.
//     * @param ticker   The stock ticker symbol.
//     * @throws IOException if an I/O error occurs.
//     */
//    private static void processTickerData(Path filePath, String ticker) throws IOException {
//        List<StockRecord> records = readCsv(filePath);
//        if (records.isEmpty()) {
//            System.err.println("Skipping empty file: " + filePath);
//            return;
//        }
//
//        // Define and calculate the target variable (profitability)
//        // This is the core of your data processing for machine learning.
//        calculateTargetVariable(records, 10, 0.05); // 5% gain within 10 days
//
//        // Save the processed data to a new CSV file
//        saveProcessedData(records, ticker);
//    }
//
//    /**
//     * Reads a CSV file and converts each row into a StockRecord object.
//     *
//     * @param filePath The path to the CSV file.
//     * @return A list of StockRecord objects.
//     * @throws IOException if the file cannot be read.
//     */
//    private static List<StockRecord> readCsv(Path filePath) throws IOException {
//        List<StockRecord> records = new ArrayList<>();
//        try (Stream<String> lines = Files.lines(filePath)) {
//            // Skip header and process the rest
//            records = lines.skip(1)
//                    .map(StockRecord::fromCsvLine)
//                    .collect(Collectors.toList());
//        }
//        return records;
//    }
//
//    /**
//     * Defines and calculates the target variable (profitability).
//     *
//     * @param records          The list of stock records.
//     * @param predictionPeriod The number of days to look ahead for a profit.
//     * @param targetGain       The percentage gain required to be considered profitable (e.g., 0.05 for 5%).
//     */
//    private static void calculateTargetVariable(List<StockRecord> records, int predictionPeriod, double targetGain) {
//        System.out.println("  -> Calculating target variable (profitability)...");
//        for (int i = 0; i < records.size() - predictionPeriod; i++) {
//            double currentClose = records.get(i).getClose();
//            double maxFuturePrice = 0.0;
//            for (int j = 1; j <= predictionPeriod; j++) {
//                if (records.get(i + j).getHigh() > maxFuturePrice) {
//                    maxFuturePrice = records.get(i + j).getHigh();
//                }
//            }
//            // If the max price in the future period is 5% or more above the current close, it's a profitable signal (1)
//            if (maxFuturePrice >= currentClose * (1 + targetGain)) {
//                records.get(i).setTarget(1);
//            } else {
//                records.get(i).setTarget(0);
//            }
//        }
//    }
//
//    /**
//     * Saves the processed data to a new CSV file.
//     *
//     * @param records The list of processed stock records.
//     * @param ticker  The stock ticker symbol.
//     * @throws IOException if the file cannot be written.
//     */
//    private static void saveProcessedData(List<StockRecord> records, String ticker) throws IOException {
//        Path filePath = PROCESSED_DATA_DIR.resolve(ticker + "_processed.csv");
//        try (FileWriter writer = new FileWriter(filePath.toFile())) {
//            // Write the header
//            writer.write("Date,Open,High,Low,Close,Volume,Target\n");
//
//            // Write the data, handling null values for initial records before calculation starts
//            for (StockRecord record : records) {
//                writer.write(record.toCsvLine() + "\n");
//            }
//        }
//    }
//}