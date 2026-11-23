package io.github.zeralius.orderflow.inventory.reservation;

import jakarta.persistence.Embeddable;

@Embeddable
public record ReservedLine(String sku, int quantity) {}
