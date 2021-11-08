package com.example.service.price;

import java.time.LocalDateTime;

public interface PriceRequester {
    PriceRecord getLastPrice(String instrumentId);

    PriceRecord getLastPrice(String instrumentId, LocalDateTime asOf);
}
