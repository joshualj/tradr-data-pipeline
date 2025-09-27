package com.tradr_data_pipeline.processor;

import com.tradr_data_pipeline.repository.ForwardFilledRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Set;

@Service
public class ForwardFilledCleanupService {

    private final ForwardFilledRepository forwardFilledRepository;

    public ForwardFilledCleanupService(ForwardFilledRepository forwardFilledRepository) {
        this.forwardFilledRepository = forwardFilledRepository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void cleanUpData() {
        BigDecimal marketCapThreshold = BigDecimal.valueOf(10_000_000_000_000L); // 1 trillion
        Set<String> problematicTickers = forwardFilledRepository.getProblematicTickers(marketCapThreshold);
        int deletedRecords = forwardFilledRepository.deleteByTickers(problematicTickers);
        System.out.printf(" - Successfully deleted %d records from ForwardFilled table with market cap >= %s.\n", deletedRecords, marketCapThreshold);
    }
}