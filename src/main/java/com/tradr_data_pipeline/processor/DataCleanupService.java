package com.tradr_data_pipeline.processor;

import com.tradr_data_pipeline.repository.AdjCloseMarketCapRepository;
import com.tradr_data_pipeline.repository.OpenHighCloseLowRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

@Service
public class DataCleanupService {

    private final OpenHighCloseLowRepository ohclRepository;
    private final AdjCloseMarketCapRepository adjCloseMarketCapRepository;

    public DataCleanupService(OpenHighCloseLowRepository ohclRepository, AdjCloseMarketCapRepository adjCloseMarketCapRepository) {
        this.ohclRepository = ohclRepository;
        this.adjCloseMarketCapRepository = adjCloseMarketCapRepository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void cleanUpData() {
        // Step 2: Clean OHCL data programmatically
        System.out.println(" - Deleting problematic OHCL records...");
        final double OHCL_LIMIT = 99999999.0;
        final BigDecimal VOLUME_LIMIT = BigDecimal.valueOf(0);
        Set<String> problematicOHCLTickers = ohclRepository.findProblematicTickers(OHCL_LIMIT, VOLUME_LIMIT);
        int ohclRowsAffected = ohclRepository.deleteByTickers(problematicOHCLTickers);
        System.out.printf(" - Successfully deleted %d records from OHCL table.\n", ohclRowsAffected);

        System.out.println(" - Finding and deleting records for tickers with negative market cap...");
        BigDecimal marketCapThreshold = BigDecimal.valueOf(10_000_000_000_000L); // 10 trillion
        Set<String> problematicTickers = adjCloseMarketCapRepository.findProblematicTickers(marketCapThreshold);
        if (!problematicTickers.isEmpty()) {
            System.out.printf(" - Found problematic tickers: %s\n", problematicTickers);
            int mcRowsAffected = adjCloseMarketCapRepository.deleteByTickers(problematicTickers);
            System.out.printf(" - Successfully deleted %d records from Market Cap table.\n", mcRowsAffected);
        } else {
            System.out.println(" - No problematic tickers found.");
        }
    }
}