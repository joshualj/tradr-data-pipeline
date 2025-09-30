package com.tradr_data_pipeline.repository;

import com.tradr_data_pipeline.model.AdjCloseMarketCapEntity;
import com.tradr_data_pipeline.model.shared.CompositeStockId;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.Set;

@Repository
public interface AdjCloseMarketCapRepository extends JpaRepository<AdjCloseMarketCapEntity, CompositeStockId> {
    @Query("SELECT DISTINCT a.compositeStockId.ticker FROM AdjCloseMarketCapEntity a WHERE a.marketCap < 0 OR a.marketCap > ?1")
    Set<String> findProblematicTickers(BigDecimal marketCapLimit);

    @Transactional
    @Modifying
    @Query("DELETE FROM AdjCloseMarketCapEntity a WHERE a.compositeStockId.ticker IN :tickers")
    int deleteByTickers(@Param("tickers") Set<String> tickers);

    /**
     * Finds a single AdjCloseMarketCap record by its composite ID.
     * This is used for targeted lookups to avoid loading the entire dataset into memory.
     *
     * @param compositeStockId The composite ID of the record to find.
     * @return An Optional containing the AdjCloseMarketCapEntity if found, otherwise an empty Optional.
     */
    Optional<AdjCloseMarketCapEntity> findByCompositeStockId(CompositeStockId compositeStockId);
}