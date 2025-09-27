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
@Table(name = "yfinance_combined_ohcl")
public class OpenHighCloseLowEntity {
    @EmbeddedId
    private CompositeStockId compositeStockId;
    @Column(name = "open")
    private double open;
    @Column(name = "close")
    private double close;
    @Column(name = "high")
    private double high;
    @Column(name = "low")
    private double low;
    @Column(name = "volume")
    private BigDecimal volume;
    @Column(name = "exchange")
    private String exchange;
    @Column(name = "update_date")
    private Date updateDate;
    @Column(name = "create_date")
    private Date createDate;
}