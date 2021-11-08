package com.example.service.price;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * This class represents the price record which includes the financial instrument id,
 * the date time of the price value and a flexible payload which includes the price value itself.
 * [DESIGN DECISION]: We use Direct Buffer Memory allocation for payload. By this way,
 * we have a better performance and do not use heap memory which is garbage collected
 * and has a low I/O performance.
 * [ASSUMPTION]: The actual price value is assumed to be the first field in the payload.
 * [ASSUMPTION]: The given date time value in the price record is assumed to be local time and we
 * do not support different time zones.
 */
public class PriceRecord {
    // [DESIGN DECISION]: The fields are made public for making them accessible easily for
    // demonstration purposes.
    public final String instrumentId;
    public final LocalDateTime asOf;
    private final ByteBuffer payload;
    private final double price;

    public PriceRecord(String instrumentId, LocalDateTime asOf, ByteBuffer payload) {
        this.instrumentId = instrumentId;
        this.asOf = asOf;
        this.payload = payload;
        this.payload.rewind();
        this.price = this.payload.getDouble();
    }

    public double getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return "[" + instrumentId + ", " + asOf.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + ", " + price + "]";
    }
}
