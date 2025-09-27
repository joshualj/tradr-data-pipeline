package com.tradr_data_pipeline.model;

//public class StockRecord {
//    private String date;
//    private double open, high, low, close, volume;
//    private Integer target;
//
//    public static StockRecord fromCsvLine(String line) {
//        String[] parts = line.split(",");
//        StockRecord record = new StockRecord();
//        record.date = parts[0];
//        record.open = Double.parseDouble(parts[1]);
//        record.high = Double.parseDouble(parts[2]);
//        record.low = Double.parseDouble(parts[3]);
//        record.close = Double.parseDouble(parts[4]);
//        record.volume = Double.parseDouble(parts[6]);
//        return record;
//    }
//
//    public String toCsvLine() {
//        return String.format("%s,%.2f,%.2f,%.2f,%.2f,%.0f,%s",
//                date, open, high, low, close, volume,
//                target != null ? target : "");
//    }
//
//    // Getters and Setters
//    public double getOpen() { return open; }
//    public double getHigh() { return high; }
//    public double getLow() { return low; }
//    public double getClose() { return close; }
//    public double getVolume() { return volume; }
//    public void setTarget(Integer target) { this.target = target; }
//}