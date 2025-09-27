package com.tradr_data_pipeline.processor;

import com.tradr_data_pipeline.model.ForwardFilledEntity;
import com.tradr_data_pipeline.model.MergeLogEntity;
import com.tradr_data_pipeline.model.MergedEntity;
import com.tradr_data_pipeline.repository.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

@Service
public class StockDataProcessorService {
    private static final Logger logger = LoggerFactory.getLogger(StockDataProcessorService.class);

    // Inject table names from application.properties
    @Value("${app.tables.merged}")
    private String mergedDataTable;
    @Value("${app.tables.ohcl}")
    private String ohclTable;
    @Value("${app.tables.market-cap}")
    private String adjCloseMarketCapTable;
    private static final String NYSE = "NYSE";
    private static final String NASDAQ = "NASDAQ";
    private final OpenHighCloseLowRepository ohclRepository;
    private final AdjCloseMarketCapRepository adjCloseMarketCapRepository;
    private final MergedRepository mergedRepository;
    private final ForwardFilledRepository forwardFilledRepository;
    private final MergeLogRepository mergeLogRepository;
    // Inject the new, dedicated cleanup service
    private final DataCleanupService dataCleanupService;
    private final JdbcTemplate jdbcTemplate; // Inject JdbcTemplate for direct SQL execution



    public StockDataProcessorService(
            OpenHighCloseLowRepository ohclRepository,
            AdjCloseMarketCapRepository adjCloseMarketCapRepository,
            MergedRepository mergedRepository,
            ForwardFilledRepository forwardFilledRepository,
            MergeLogRepository mergeLogRepository,
            DataCleanupService dataCleanupService,
            JdbcTemplate jdbcTemplate
    ) {
        this.ohclRepository = ohclRepository;
        this.adjCloseMarketCapRepository = adjCloseMarketCapRepository;
        this.mergedRepository = mergedRepository;
        this.forwardFilledRepository = forwardFilledRepository;
        this.mergeLogRepository = mergeLogRepository;
        this.dataCleanupService = dataCleanupService;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    //@EventListener(ApplicationReadyEvent.class)
    public void runDataCleanupAndMerge() {
        System.out.println("--- Starting data cleaning and merge process ---");

        // Step 1: Determine the start date for the load
        LocalDateTime lastRunDate = null;
        MergeLogEntity lastLog = mergeLogRepository.findFirstByOrderByRunDateDesc();
        if (lastLog != null) {
            lastRunDate = lastLog.getRunDate();
            logger.info(" - Resuming from last successful run date: {}", lastRunDate);
        } else {
            logger.info(" - First run detected. Processing all data.");
        }

        dataCleanupService.cleanUpData();

        // Step 4: Reset and rebuild the merged table with the cleaned data
        rebuildMergedTable();

        //forwardFillMarketCapData(lastRunDate);

            // Step 7: Log the successful run
        mergeLogRepository.save(new MergeLogEntity(LocalDateTime.now()));

//        logger.info(" - Successfully merged and saved {} records to the final table.", mergedCount[0]);
        logger.info("--- Data processing complete ---");
    }

    /**
     * Resets and rebuilds the yfinance_merged_data table by performing an atomic swap.
     * This is safer than a simple DROP/CREATE sequence, ensuring data availability.
     */
    private void rebuildMergedTable() {
        System.out.println(" - Resetting and rebuilding merged data table...");

        // Step 1: Create a temporary table with the new data.
        // This is a single, multi-line SQL statement executed as a string.
        // Step 1: Create a temporary table with the new data.
        String tempName = "yfinance_merged_data_temp";
        String createTempTableSql = String.format(
                "CREATE TABLE %s_new AS " +
                        "SELECT " +
                        "    ohcl.ticker, " +
                        "    ohcl.date, " +
                        "    ohcl.open, " +
                        "    ohcl.high, " +
                        "    ohcl.low, " +
                        "    ohcl.close, " +
                        "    ohcl.volume, " +
                        "    adj_close.market_capitalization, " +
                        "    CURRENT_TIMESTAMP AS create_date, " +
                        "    CURRENT_TIMESTAMP AS update_date " +
                        "FROM " +
                        "    %s AS ohcl " +
                        "LEFT JOIN " +
                        "    %s AS adj_close " +
                        "    ON ohcl.ticker = adj_close.ticker AND ohcl.date = adj_close.date;",
                tempName, ohclTable, adjCloseMarketCapTable
        );
        jdbcTemplate.execute(createTempTableSql);

        // Step 2: Drop the old table using the dynamic name.
        jdbcTemplate.execute(String.format("DROP TABLE IF EXISTS %s;", mergedDataTable));

        // Step 3: Rename the new table to the final name.
        jdbcTemplate.execute(String.format("ALTER TABLE %s_new RENAME TO %s;", tempName, mergedDataTable));

        System.out.println(" - Merged data table successfully rebuilt.");
    }

    private void forwardFillMarketCapData(LocalDateTime startDate) {
        // This is a safe operation as the Set of tickers is small
        logger.info(" - Fetching unique tickers from the merged table...");
        Set<String> uniqueTickers = (startDate != null) ? mergedRepository.findAllTickersAfterDate(startDate) : mergedRepository.findAllTickers();
        logger.info(" - Found {} unique tickers to process.", uniqueTickers.size());
        AtomicLong totalProcessedRecords = new AtomicLong();

        for (String ticker : uniqueTickers) {
            logger.info(" - Processing ticker: {}", ticker);

            // Fetch all records for the current ticker into memory. This is the key change
            // to enable a single, large database commit for this ticker.
            List<MergedEntity> mergedRecords = mergedRepository.findAllByTickerOrderByDate(ticker);

            if (mergedRecords.isEmpty()) {
                logger.info(" - No records found for ticker {}. Skipping.", ticker);
                continue;
            }

            // A temporary list to hold the forward-filled data before the bulk save.
            List<ForwardFilledEntity> filledEntities = new ArrayList<>(mergedRecords.size());
            AtomicReference<BigDecimal> lastKnownMarketCap = new AtomicReference<>();

            // This is the core forward-filling logic, happening in a single pass over
            // the in-memory list.
            for (MergedEntity record : mergedRecords) {
                if (record.getMarketCap() != null) {
                    lastKnownMarketCap.set(record.getMarketCap());
                }

                // Create a new entity with the filled market cap. We do not modify the original
                // MergedEntity record, but create a new one for the target table.
                ForwardFilledEntity filledEntity = new ForwardFilledEntity(record);
                filledEntity.setMarketCap(lastKnownMarketCap.get());
                filledEntities.add(filledEntity);
            }

            // Save all records for the current ticker in a single bulk transaction.
            forwardFilledRepository.saveAll(filledEntities);

            totalProcessedRecords.addAndGet(filledEntities.size());
            logger.info(" - Processed and saved {} records for ticker {}.", filledEntities.size(), ticker);
        }
    }
}




// September 21, 2025
//final int BATCH_SIZE = 1000;
//
//// Iterate through each ticker to process its data in a memory-safe way
//        for (String ticker : uniqueTickers) {
//        logger.info(" - Processing ticker: {}", ticker);
//List<ForwardFilledEntity> batch = new ArrayList<>(BATCH_SIZE);
//AtomicReference<BigDecimal> lastKnownMarketCap = new AtomicReference<>();
//AtomicLong processedRecordsForTicker = new AtomicLong(); // New counter for the current ticker
//
//// Stream the records for the current ticker. This prevents loading all records at once.
//            try (Stream<MergedEntity> mergedRecords = mergedRepository.streamAllByTickerOrderByDate(ticker)) {
//        mergedRecords.forEach(record -> {
//        // This is the core forward-filling logic, happening in a single pass.
//        if (record.getMarketCap() != null) {
//        lastKnownMarketCap.set(record.getMarketCap());
//        } else {
//        record.setMarketCap(lastKnownMarketCap.get());
//        }
//        batch.add(new ForwardFilledEntity(record));
//
//        // Save in batches to prevent OutOfMemoryError
//        if (batch.size() >= BATCH_SIZE) {
//        forwardFilledRepository.saveAll(batch);
//                        totalProcessedRecords.addAndGet(batch.size());
//        processedRecordsForTicker.addAndGet(batch.size());
//        batch.clear();
//                    }
//                            });
//                            }
//
//                            // Save any remaining records in the last batch for the current ticker
//                            if (!batch.isEmpty()) {
//        forwardFilledRepository.saveAll(batch);
//                totalProcessedRecords.addAndGet(batch.size());
//        processedRecordsForTicker.addAndGet(batch.size());
//        }



//// Step 3: Pre-load the AdjCloseMarketCap data into a Map for fast lookups.
//        // NOTE: This assumes the AdjCloseMarketCap table is not large enough to cause an OutOfMemoryError.
//        // For a very large table, a different streaming approach would be required.
//        logger.info(" - Pre-loading all market cap data into memory...");
//Map<CompositeStockId, AdjCloseMarketCapEntity> adjCloseMarketCapMap = adjCloseMarketCapRepository.findAll().stream()
//        .collect(Collectors.toMap(AdjCloseMarketCapEntity::getCompositeStockId, e -> e));
//        logger.info(" - Successfully pre-loaded {} market cap records.", adjCloseMarketCapMap.size());
//
//// A cache to store the last known market cap for each ticker, used for forward-filling.
//Map<String, BigDecimal> lastKnownMarketCapCache = new HashMap<>();
//
//// Step 4: Stream the OHCL data for memory-efficient processing
//final int BATCH_SIZE = 1000;
//final int[] mergedCount = {0}; // Use an array to make it effectively final for the lambda
//List<MergedEntity> batch = new ArrayList<>(BATCH_SIZE);
//
//        logger.info(" - Merging and forward-filling data by streaming OHCL records...");
//        try (Stream<OpenHighCloseLowEntity> ohclStream = lastRunDate == null ?
//        ohclRepository.streamAllOrderByTickerAndDate() :
//        ohclRepository.streamAllByDateGreaterThanOrderByTickerAndDate(lastRunDate.toLocalDate())) {
//
//        ohclStream.forEach(ohclRecord -> {
//String ticker = ohclRecord.getCompositeStockId().getTicker();
//LocalDate date = ohclRecord.getCompositeStockId().getDate();
//CompositeStockId compositeStockId = new CompositeStockId(ticker, date);
//
//// Step 5: Look up market cap from the in-memory map instead of querying the database
//AdjCloseMarketCapEntity adjCloseMarketCapRecord = adjCloseMarketCapMap.get(compositeStockId);
//BigDecimal currentMarketCap = null;
//
//                if (adjCloseMarketCapRecord != null) {
//currentMarketCap = adjCloseMarketCapRecord.getMarketCap();
//// Update the cache with the newly found market cap
//                    lastKnownMarketCapCache.put(ticker, currentMarketCap);
//                } else {
//// If no record exists for this date, use the last known market cap from the cache (forward-fill).
//currentMarketCap = lastKnownMarketCapCache.get(ticker);
//                }
//
//                        if (currentMarketCap != null) {
//MergedEntity finalEntity = new MergedEntity(
//        new CompositeStockId(ticker, date),
//        ohclRecord.getOpen(),
//        ohclRecord.getClose(),
//        ohclRecord.getHigh(),
//        ohclRecord.getLow(),
//        ohclRecord.getVolume(),
//        currentMarketCap
//);
//                    batch.add(finalEntity);
//                }
//
//                        // Step 6: Save in batches to prevent OutOfMemoryError
//                        if (batch.size() >= BATCH_SIZE) {
//        mergedRepository.saveAll(batch);
//mergedCount[0] += batch.size();
//                    batch.clear();
//                }
//                        });
//
//                        if (mergedCount[0] % 100000 == 0) {
//        logger.info(" - Processed {} records so far at {}...", mergedCount[0], LocalDateTime.now());
//        }
//
//        // Save any remaining records in the last batch
//        if (!batch.isEmpty()) {
//        mergedRepository.saveAll(batch);
//mergedCount[0] += batch.size();
//            }
//        } catch (Exception e) {
//        logger.error("An error occurred during data merge and forward-fill process.", e);
//            throw new RuntimeException("Data processing failed", e);
//        }


// September 20, 2025
//        // Step 4: Merge and forward-fill data
//        System.out.println("  - Merging and forward-filling data...");
//
////        List<OpenHighCloseLowEntity> ohclData = ohclRepository.findAll();
//        //List<AdjCloseMarketCapEntity> adjCloseMarketCapData = adjCloseMarketCapRepository.findAll();
//
//        // Group market cap data by CompositeStockId for efficient lookups
//        //Map<CompositeStockId, AdjCloseMarketCapEntity> adjCloseMarketCapMap = adjCloseMarketCapData.stream()
//                //.collect(Collectors.toMap(AdjCloseMarketCapEntity::getCompositeStockId, e -> e));
//
////        List<MergedEntity> finalData = new ArrayList<>();
//        // Step 5 Stream the OHCL data and process it one record at a time
//        // This is the core change to prevent OutOfMemoryError
//        logger.info(" - Merging and forward-filling data by streaming OHCL records...");
//        // Use an AtomicReference to hold the last known market cap value
//        final AtomicReference<BigDecimal> lastKnownMarketCap = new AtomicReference<>();
//        final int BATCH_SIZE = 1000;
//        final AtomicInteger mergedCount = new AtomicInteger(0);
//        List<MergedEntity> batch = new ArrayList<>(BATCH_SIZE);
//
//        try (Stream<OpenHighCloseLowEntity> ohclStream = lastRunDate == null ?
//                ohclRepository.streamAllOrderByTickerAndDate() :
//                ohclRepository.streamAllByDateGreaterThanOrderByTickerAndDate(lastRunDate.toLocalDate())) {
//
//            ohclStream.forEach(ohclRecord -> {
//                String ticker = ohclRecord.getCompositeStockId().getTicker();
//                LocalDate date = ohclRecord.getCompositeStockId().getDate();
//                CompositeStockId compositeStockId = new CompositeStockId(ticker, date);
//
//                // Look up the market cap record individually from the database
//                Optional<AdjCloseMarketCapEntity> adjCloseMarketCapRecordOptional = adjCloseMarketCapRepository.findByCompositeStockId(compositeStockId);
//                BigDecimal currentMarketCap = adjCloseMarketCapRecordOptional.map(AdjCloseMarketCapEntity::getMarketCap).orElse(null);
//
//                if (currentMarketCap != null) {
//                    lastKnownMarketCap.set(currentMarketCap);
//                } else {
//                    currentMarketCap = lastKnownMarketCap.get();
//                }
//
//                if (currentMarketCap != null) {
//                    MergedEntity finalEntity = new MergedEntity(
//                            new CompositeStockId(ticker, date),
//                            ohclRecord.getOpen(),
//                            ohclRecord.getClose(),
//                            ohclRecord.getHigh(),
//                            ohclRecord.getLow(),
//                            ohclRecord.getVolume(),
//                            currentMarketCap
//                    );
//                    batch.add(finalEntity);
//                }
//
//                // Save in batches to prevent another OutOfMemoryError on the saveAll()
//                if (batch.size() >= BATCH_SIZE) {
//                    mergedRepository.saveAll(batch);
//                    mergedCount.addAndGet(batch.size());
//                    batch.clear();
//                    logger.info(" - Saved a batch. Total merged records: {}", mergedCount.get());
//                }
//            });
//
//            // Save any remaining records in the last batch
//            if (!batch.isEmpty()) {
//                mergedRepository.saveAll(batch);
//                mergedCount.addAndGet(batch.size());
//            }
//            // Step 6: Log the successful run
//
//            mergeLogRepository.save(new MergeLogEntity(LocalDateTime.now()));
//
//
//        } catch (Exception e) {
//            logger.error("An error occurred during data merge and forward-fill process.", e);
//            throw new RuntimeException("Data processing failed", e);
//        }
//
//        logger.info(" - Successfully merged and saved {} records to the final table.", mergedCount);
//        logger.info("--- Data processing complete ---");
//    }
//}








// New, separate transactional method to ensure cleanup operations are committed
//    @Transactional
//    public void cleanUpData() {
//        // Step 2: Clean OHCL data programmatically
//        System.out.println(" - Deleting problematic OHCL records...");
//        int ohclRowsAffected = ohclRepository.deleteProblematicRecords();
//        System.out.printf(" - Successfully deleted %d records from OHCL table.\n", ohclRowsAffected);
//
//        // Step 3: Clean Market Cap data programmatically
//        System.out.println(" - Finding and deleting records for tickers with negative market cap...");
//        List<String> problematicTickers = adjCloseMarketCapRepository.findProblematicTickers();
//        if (!problematicTickers.isEmpty()) {
//            System.out.printf(" - Found problematic tickers: %s\n", problematicTickers);
//            int mcRowsAffected = adjCloseMarketCapRepository.deleteByTickers(problematicTickers);
//            System.out.printf(" - Successfully deleted %d records from Market Cap table.\n", mcRowsAffected);
//        } else {
//            System.out.println(" - No problematic tickers found.");
//        }
//    }

//        Map<String, BigDecimal> lastKnownMarketCap = new HashMap<>();
//
//        // Sort OHCL data by ticker and date to ensure correct forward-filling
//        ohclData.sort(Comparator.comparing((OpenHighCloseLowEntity e) -> e.getCompositeStockId().getTicker())
//                .thenComparing(e -> e.getCompositeStockId().getDate()));
//
//        for (OpenHighCloseLowEntity ohclRecord : ohclData) {
//            String ticker = ohclRecord.getCompositeStockId().getTicker();
//            LocalDate date = ohclRecord.getCompositeStockId().getDate();
//
//            // Find the corresponding market cap record
//            AdjCloseMarketCapEntity adjCloseMarketCapRecord = adjCloseMarketCapMap.get(new CompositeStockId(ticker, date));
//
//            BigDecimal currentMarketCap = null;
//            if (adjCloseMarketCapRecord != null) {
//                currentMarketCap = adjCloseMarketCapRecord.getMarketCap();
//                // Update the last known market cap for this ticker
//                lastKnownMarketCap.put(ticker, currentMarketCap);
//            } else {
//                // Forward-fill if no market cap record exists for this date
//                currentMarketCap = lastKnownMarketCap.get(ticker);
//            }
//
//            // Only merge if we have a valid market cap value, either from the record or from forward-filling.
//            if (currentMarketCap != null) {
//                // Create the new combined entity and add it to the list
//                MergedEntity finalEntity = new MergedEntity(
//                        new CompositeStockId(ticker, date),
//                        ohclRecord.getOpen(),
//                        ohclRecord.getClose(),
//                        ohclRecord.getHigh(),
//                        ohclRecord.getLow(),
//                        ohclRecord.getVolume(),
//                        currentMarketCap
//                );
//                finalData.add(finalEntity);
//            }
//        }
//
//        // Save all the merged and processed data to the new table
//        mergedRepository.saveAll(finalData);
//        System.out.printf("  - Successfully merged and saved %d records to the final table.\n", finalData.size());
//
//        System.out.println("--- Data processing complete ---");

//    private void cleanByTicker(String exchange) {
//        Map<String, List<OpenHighCloseLowEntity>> tickerToOHCLRecords = getOHCLRecordsByTicker(exchange);
//TODO: add helper method to remove key-value pair if list < 2
//TODO: get eps records by ticker as tickerToEPSRecords
//TODO: for each ticker that exists in both tickerToOHCLRecords and tickerToEPSRecords,
// TODO:   merge the two lists by date
//    }