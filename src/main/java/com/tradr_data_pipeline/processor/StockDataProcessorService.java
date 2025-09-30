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
    @Value("${app.tables.temp-merged}")
    private String tempMergedTable;
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
                tempMergedTable, ohclTable, adjCloseMarketCapTable
        );
        jdbcTemplate.execute(createTempTableSql);

        // Step 2: Drop the old table using the dynamic name.
        jdbcTemplate.execute(String.format("DROP TABLE IF EXISTS %s;", mergedDataTable));

        // Step 3: Rename the new table to the final name.
        jdbcTemplate.execute(String.format("ALTER TABLE %s_new RENAME TO %s;", tempMergedTable, mergedDataTable));

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