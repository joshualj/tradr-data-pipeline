package com.tradr_data_pipeline.repository;

import com.tradr_data_pipeline.model.MergedEntity;
import com.tradr_data_pipeline.model.shared.CompositeStockId;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

@Repository
public interface MergedRepository extends JpaRepository<MergedEntity, CompositeStockId> {
    // Custom query to find all unique tickers. This is a lightweight and safe operation.
    @Query("SELECT DISTINCT m.compositeStockId.ticker FROM MergedEntity m")
    Set<String> findAllTickers();

    // The key to a memory-safe approach: stream the results for a single ticker.
    // This processes data one by one, avoiding loading the entire list into memory.
    @Query("SELECT m FROM MergedEntity m WHERE m.compositeStockId.ticker = :ticker ORDER BY m.compositeStockId.date")
    Stream<MergedEntity> streamAllByTickerOrderByDate(@Param("ticker") String ticker);

    @Query("SELECT m FROM MergedEntity m WHERE m.compositeStockId.ticker = :ticker ORDER BY m.compositeStockId.date")
    List<MergedEntity> findAllByTickerOrderByDate(@Param("ticker") String ticker);

    // New query to find unique tickers that have been updated since the last run.
    @Query("SELECT DISTINCT m.compositeStockId.ticker FROM MergedEntity m WHERE m.compositeStockId.date > :date")
    Set<String> findAllTickersAfterDate(@Param("date") LocalDateTime date);
}