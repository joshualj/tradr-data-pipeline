package com.tradr_data_pipeline.model;

import com.tradr_data_pipeline.model.shared.CompositeStockId;
import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "yfinance_combined_better_adj_close_market_cap")
public class AdjCloseMarketCapEntity {
    @EmbeddedId
    private CompositeStockId compositeStockId;
    @Column(name = "close_adjusted")
    private double closeAdjusted;
    @Column(name = "market_capitalization")
    private BigDecimal marketCap;
    @Column(name = "exchange")
    private String exchange;
    @Column(name = "update_date")
    private Date updateDate;
    @Column(name = "create_date")
    private Date createDate;
}