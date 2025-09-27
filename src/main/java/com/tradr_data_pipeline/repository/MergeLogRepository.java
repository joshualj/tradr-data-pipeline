package com.tradr_data_pipeline.repository;

import com.tradr_data_pipeline.model.MergeLogEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MergeLogRepository extends JpaRepository<MergeLogEntity, Long> {
    /**
     * Finds the most recent successful run date from the merge log.
     * @return the most recent MergeLogEntity, or null if no records exist.
     */
    MergeLogEntity findFirstByOrderByRunDateDesc();
}
