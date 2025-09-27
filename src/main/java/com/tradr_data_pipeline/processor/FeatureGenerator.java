package com.tradr_data_pipeline.processor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//public class FeatureGenerator {
//
//    private static final Path PROCESSED_DATA_DIR = Paths.get("data", "processed_data");
//    private static final Path FINAL_DATASET_DIR = Paths.get("data", "final_dataset");
//
//    // Simple class to hold stock data and features.
//    // This is included to make the code self-contained.
//    private static class StockRecord {
//        String date;
//        double open, high, low, close, volume;
//        int target;
//        Double sma_20, ema_12, ema_26, rsi_14;
//
//        // Factory method to create a StockRecord from a CSV line.
//        public static StockRecord fromCsvLine(String line) {
//            String[] parts = line.split(",");
//            StockRecord record = new StockRecord();
//            record.date = parts[0];
//            record.open = Double.parseDouble(parts[1]);
//            record.high = Double.parseDouble(parts[2]);
//            record.low = Double.parseDouble(parts[3]);
//            record.close = Double.parseDouble(parts[4]);
//            record.volume = Double.parseDouble(parts[5]);
//            record.target = Integer.parseInt(parts[6]);
//            return record;
//        }
//
//        public String toCsvLine() {
//            return String.format("%s,%.2f,%.2f,%.2f,%.2f,%.0f,%d,%.2f,%.2f,%.2f,%.2f",
//                    date, open, high, low, close, volume, target, sma_20, ema_12, ema_26, rsi_14);
//        }
//    }
//
//    public static void main(String[] args) {
//        System.out.println("Starting feature generation...");
//
//        try {
//            Files.createDirectories(FINAL_DATASET_DIR);
//
//            List<Path> csvFiles = Files.walk(PROCESSED_DATA_DIR)
//                    .filter(Files::isRegularFile)
//                    .filter(path -> path.toString().endsWith("_processed.csv"))
//                    .collect(Collectors.toList());
//
//            if (csvFiles.isEmpty()) {
//                System.err.println("No processed data files found in " + PROCESSED_DATA_DIR);
//                return;
//            }
//
//            for (Path filePath : csvFiles) {
//                String ticker = filePath.getFileName().toString().replace("_processed.csv", "");
//                System.out.println("Generating features for " + ticker);
//                generateFeaturesForTicker(filePath, ticker);
//            }
//
//            System.out.println("Feature generation complete. Check the 'data/final_dataset' directory.");
//
//        } catch (IOException e) {
//            System.err.println("An error occurred during feature generation: " + e.getMessage());
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * Reads a processed CSV, calculates features, and saves the final dataset.
//     *
//     * @param filePath The path to the processed CSV file.
//     * @param ticker   The stock ticker symbol.
//     * @throws IOException if an I/O error occurs.
//     */
//    private static void generateFeaturesForTicker(Path filePath, String ticker) throws IOException {
//        List<StockRecord> records = readProcessedCsv(filePath);
//        if (records.isEmpty()) {
//            return;
//        }
//
//        // Calculate technical indicators
//        //calculateSMA(records, 20);
//        calculateEMA(records, 12);
//        calculateEMA(records, 26);
//        //calculateRSI(records, 14);
//
//        // Save the final dataset
//        saveFinalDataset(records, ticker);
//    }
//
//    /**
//     * Reads the processed CSV file into a list of StockRecord objects.
//     *
//     * @param filePath The path to the processed CSV file.
//     * @return A list of StockRecord objects.
//     * @throws IOException if the file cannot be read.
//     */
//    private static List<StockRecord> readProcessedCsv(Path filePath) throws IOException {
//        List<StockRecord> records = new ArrayList<>();
//        try (Stream<String> lines = Files.lines(filePath)) {
//            // Skip the header and process the rest.
//            records = lines.skip(1)
//                    .map(StockRecord::fromCsvLine)
//                    .collect(Collectors.toList());
//        }
//        return records;
//    }
//
//
//    /**
//     * Calculates the Exponential Moving Average (EMA).
//     *
//     * @param records The list of stock records.
//     * @param period  The period for the EMA calculation.
//     */
//    private static void calculateEMA(List<StockRecord> records, int period) {
//        System.out.println("  -> Calculating " + period + "-day EMA...");
//        double multiplier = 2.0 / (period + 1);
//        Double ema = null;
//
//        for (StockRecord record : records) {
//            if (ema == null) {
//                // Initialize the EMA with the first closing price.
//                ema = record.close;
//            } else {
//                ema = (record.close - ema) * multiplier + ema;
//            }
//            if (period == 12) {
//                record.ema_12 = ema;
//            } else if (period == 26) {
//                record.ema_26 = ema;
//            }
//        }
//    }
//
//    /**
//     * Saves the final dataset with all features to a new CSV file.
//     *
//     * @param records The list of stock records with features.
//     * @param ticker  The stock ticker symbol.
//     * @throws IOException if the file cannot be written.
//     */
//    private static void saveFinalDataset(List<StockRecord> records, String ticker) throws IOException {
//        Path filePath = FINAL_DATASET_DIR.resolve(ticker + "_final_dataset.csv");
//        try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
//            // Write the header
//            writer.write("Date,Open,High,Low,Close,Volume,Target,SMA_20,EMA_12,EMA_26,RSI_14\n");
//
//            // Write the data
//            for (StockRecord record : records) {
//                // Only write records that have all features calculated (after the longest period)
//                if (record.sma_20 != null && record.ema_12 != null && record.ema_26 != null && record.rsi_14 != null) {
//                    writer.write(record.toCsvLine() + "\n");
//                }
//            }
//            System.out.println("  -> Successfully saved final dataset to " + filePath);
//        }
//    }
//}