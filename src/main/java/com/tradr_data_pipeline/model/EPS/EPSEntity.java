package com.tradr_data_pipeline.model.EPS;

import com.tradr_data_pipeline.model.shared.CompositeStockId;
import jakarta.persistence.EmbeddedId;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EPSEntity {
    @EmbeddedId
    private CompositeStockId compositeStockId;
    private BigDecimal eps;
}