package com.tradr_data_pipeline;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//public class StockDataDownloader {

    // Define constants for the NASDAQ file URL and local file paths
    // *** The FTP link has been replaced with the modern HTTPS equivalent. ***
//    private static final String NASDAQ_TICKER_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt";
//    private static final Path DATA_DIR = Paths.get("data");
//    private static final Path TICKER_FILE_PATH = DATA_DIR.resolve("nasdaqlisted.txt");
//    private static final Path HISTORICAL_DATA_DIR = DATA_DIR.resolve("historical_data");
//    private static final int RATE_LIMIT_DELAY_MS = 5000;
//
//
//    // Use a modern, non-blocking HTTP client
//    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
//
//    public static void main(String[] args) {
//        System.out.println("Starting stock data acquisition pipeline...");
//
//        try {
//            // Ensure necessary directories exist
//            Files.createDirectories(HISTORICAL_DATA_DIR);
//
//            // Step 1: Download the list of NASDAQ tickers
//            downloadNasdaqTickers();
//
//            // Step 2: Parse the downloaded file to get a list of tickers
//            List<String> tickers = parseTickers();
//            System.out.println("Found " + tickers.size() + " tickers.");
//
//            // Step 3: Download historical data for each ticker
//            // This is a simulated download to demonstrate the logic.
//            // Replace with your actual API calls.
//            downloadHistoricalData(tickers);
//
//            System.out.println("Data acquisition complete. Check the 'data' directory.");
//        } catch (IOException | InterruptedException e) {
//            System.err.println("An error occurred during the data acquisition process: " + e.getMessage());
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * Downloads the latest list of NASDAQ tickers from the official NASDAQ Trader website.
//     * @throws IOException if an I/O error occurs.
//     * @throws InterruptedException if the operation is interrupted.
//     */
//    private static void downloadNasdaqTickers() throws IOException, InterruptedException {
//        System.out.println("Downloading NASDAQ ticker list...");
//        HttpRequest request = HttpRequest.newBuilder()
//                .uri(URI.create(NASDAQ_TICKER_URL))
//                .GET()
//                .build();
//
//        HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
//
//        if (response.statusCode() == 200) {
//            Files.writeString(TICKER_FILE_PATH, response.body());
//            System.out.println("Successfully downloaded ticker list to " + TICKER_FILE_PATH);
//        } else {
//            System.err.println("Failed to download ticker list. Status code: " + response.statusCode());
//        }
//    }
//
//    /**
//     * Parses the downloaded NASDAQ ticker file to extract a list of stock symbols.
//     * @return A List of stock ticker symbols.
//     * @throws IOException if the ticker file cannot be read.
//     */
//    private static List<String> parseTickers() throws IOException {
//        System.out.println("Parsing tickers from " + TICKER_FILE_PATH);
//
//        List<String> tickers = new ArrayList<>();
//        try (Stream<String> lines = Files.lines(TICKER_FILE_PATH)) {
//            // Skip the header and footer lines
//            tickers = lines.skip(1)
//                    .filter(line -> !line.startsWith("File Creation Time:"))
//                    .map(line -> line.split("\\|")[0]) // The ticker is the first column
//                    .collect(Collectors.toList());
//        }
//        return tickers;
//    }
//
//    /**
//     * Downloads historical data for each ticker from a public source and saves it
//     * as a CSV file.
//     *
//     * @param tickers The list of ticker symbols.
//     * @throws InterruptedException if the thread is interrupted during sleep.
//     */
//    private static void downloadHistoricalData(List<String> tickers) throws InterruptedException {
//        System.out.println("Beginning download of historical data...");
//
//        long period1 = 1262304000L; // Unix timestamp for Jan 1, 2010
//        long period2 = System.currentTimeMillis() / 1000L; // Current Unix timestamp
//        final String historicalDataUrlTemplate = "https://query1.finance.yahoo.com/v7/finance/download/%s?period1=" + period1 + "&period2=" + period2 + "&interval=1d&events=history&includeAdjustedClose=true";
//
//        for (String ticker : tickers) {
//            String url = String.format(historicalDataUrlTemplate, ticker);
//            Path filePath = HISTORICAL_DATA_DIR.resolve(ticker + ".csv");
//            int backoffDelay = RATE_LIMIT_DELAY_MS;
//
//            // Simple retry loop (e.g., up to 3 times)
//            for (int attempt = 0; attempt < 3; attempt++) {
//                try {
//                    System.out.println("Downloading data for " + ticker + " (Attempt " + (attempt + 1) + ")");
//
//                    HttpRequest request = HttpRequest.newBuilder()
//                            .uri(URI.create(url))
//                            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
//                                    "AppleWebKit/537.36 (KHTML, like Gecko) " +
//                                    "Chrome/116.0.0.0 Safari/537.36")
//                            .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
//                            .header("Accept-Language", "en-US,en;q=0.5")
//                            .GET()
//                            .build();
//
//                    HttpResponse<Path> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofFile(filePath));
//
//                    if (response.statusCode() == 200) {
//                        System.out.println("  -> Successfully saved to " + filePath);
//                        break;
//                    } else if (response.statusCode() == 429) {
//                        // Special handling for rate-limiting with exponential backoff
//                        System.err.println("  -> Failed to download data for " + ticker + ". Status code: " + response.statusCode());
//                        System.err.println("  -> Encountered rate limit. Retrying in " + (backoffDelay / 1000) + " seconds...");
//                        Thread.sleep(backoffDelay);
//                        backoffDelay *= 2; // Exponentially increase the delay
//                    } else {
//                        // Handle other non-200 status codes
//                        System.err.println("  -> Failed to download data for " + ticker + ". Status code: " + response.statusCode());
//                        if (attempt < 2) {
//                            System.err.println("  -> Retrying in 5 seconds...");
//                            Thread.sleep(5000);
//                        }
//                    }
//                } catch (IOException e) {
//                    System.err.println("  -> Failed to download data for " + ticker + ": " + e.getMessage());
//                    if (attempt < 2) {
//                        System.err.println("  -> Retrying in 5 seconds...");
//                        Thread.sleep(5000);
//                    }
//                }
//            }
//
//            // Implement a rate limit delay between each ticker to prevent API abuse.
//            Thread.sleep(RATE_LIMIT_DELAY_MS);
//        }
//    }
//}