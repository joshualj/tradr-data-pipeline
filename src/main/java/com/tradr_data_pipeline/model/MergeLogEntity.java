package com.tradr_data_pipeline.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "merged_log_data")
public class MergeLogEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "run_date", nullable = false, updatable = false)
    private LocalDateTime runDate;

    public MergeLogEntity(LocalDateTime runDate) {
        this.runDate = runDate;
    }
}