package com.tradr_data_pipeline.repository;

import com.tradr_data_pipeline.model.OpenHighCloseLowEntity;
import com.tradr_data_pipeline.model.shared.CompositeStockId;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.Set;
import java.util.stream.Stream;

@Repository
public interface OpenHighCloseLowRepository extends JpaRepository<OpenHighCloseLowEntity, CompositeStockId> {
    @Transactional
    @Modifying
    @Query("DELETE FROM OpenHighCloseLowEntity o WHERE o.open > 99999999 OR (o.open = o.high AND o.high = o.low AND o.low = o.close) OR o.volume = 0")
    int deleteProblematicRecords();

    @Transactional
    @Modifying
    @Query("SELECT DISTINCT o.compositeStockId.ticker FROM OpenHighCloseLowEntity o WHERE " +
            "o.open >= ?1 OR " +
            "o.close >= ?1 OR " +
            "o.high >= ?1 OR " +
            "o.low >= ?1 OR " +
            "(o.open = o.high AND o.high = o.low AND o.low = o.close) OR " +
            "o.volume <= ?2")
    Set<String> findProblematicTickers(double ohclLimit, BigDecimal volumeLimit);

    @Transactional
    @Modifying
    @Query("DELETE FROM OpenHighCloseLowEntity o WHERE o.compositeStockId.ticker IN :tickers")
    int deleteByTickers(@Param("tickers") Set<String> tickers);

    @Query("SELECT o FROM OpenHighCloseLowEntity o ORDER BY o.compositeStockId.ticker, o.compositeStockId.date")
    Stream<OpenHighCloseLowEntity> streamAllOrderByTickerAndDate();

    /**
     * Streams OHLCV records from the database that are newer than the specified date,
     * ordered by ticker and date.
     * This is crucial for incremental change data capture (CDC).
     *
     * @param date The date to start streaming data from.
     * @return A Stream of OHLCVEntity objects with dates greater than the provided date.
     */
    @Query("SELECT o FROM OpenHighCloseLowEntity o WHERE o.compositeStockId.date > ?1 ORDER BY o.compositeStockId.ticker, o.compositeStockId.date")
    Stream<OpenHighCloseLowEntity> streamAllByDateGreaterThanOrderByTickerAndDate(LocalDate date);
}