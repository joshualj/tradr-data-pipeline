package com.tradr_data_pipeline.repository;

import com.tradr_data_pipeline.model.ForwardFilledEntity;
import com.tradr_data_pipeline.model.MergedEntity;
import com.tradr_data_pipeline.model.shared.CompositeStockId;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.stream.Stream;

@Repository
public interface ForwardFilledRepository extends JpaRepository<ForwardFilledEntity, CompositeStockId> {
    @Transactional
    @Modifying
    @Query("SELECT DISTINCT f.compositeStockId.ticker FROM ForwardFilledEntity f where f.marketCap >= ?1")
    Set<String> getProblematicTickers(BigDecimal marketCapLimit);

    @Transactional
    @Modifying
    @Query("DELETE FROM ForwardFilledEntity f WHERE f.compositeStockId.ticker IN :tickers")
    int deleteByTickers(@Param("tickers") Set<String> tickers);
}