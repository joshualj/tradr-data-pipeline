package com.tradr_data_pipeline.model;

import com.tradr_data_pipeline.model.shared.CompositeStockId;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
//@Table(name = "merged_stock_data")
@Table(name = "yfinance_merged_data",
        indexes = {
                @Index(name = "idx_ticker_date", columnList = "ticker, compositeStockId_date")
        })
public class MergedEntity {
    @EmbeddedId
    private CompositeStockId compositeStockId;
    private double open;
    private double close;
    private double high;
    private double low;
    private BigDecimal volume;
    @Column(name = "market_capitalization")
    private BigDecimal marketCap;
    @Column(name = "create_date", nullable = false, updatable = false)
    private LocalDateTime createDate;

    @Column(name = "update_date", nullable = false)
    private LocalDateTime updateDate;

    // JPA callback to set creation date before persisting
    @PrePersist
    protected void onCreate() {
        this.createDate = LocalDateTime.now();
        this.updateDate = LocalDateTime.now();
    }

    // JPA callback to set update date before an update
    @PreUpdate
    protected void onUpdate() {
        this.updateDate = LocalDateTime.now();
    }

    public MergedEntity(CompositeStockId compositeStockId, double open, double close, double high, double low, BigDecimal volume, BigDecimal marketCap) {
        this.compositeStockId = compositeStockId;
        this.open = open;
        this.close = close;
        this.high = high;
        this.low = low;
        this.volume = volume;
        this.marketCap = marketCap;
    }
}