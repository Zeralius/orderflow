package io.github.zeralius.orderflow.order.domain;

import jakarta.persistence.Embeddable;

@Embeddable
public record OrderLine(String sku, int quantity) {}
