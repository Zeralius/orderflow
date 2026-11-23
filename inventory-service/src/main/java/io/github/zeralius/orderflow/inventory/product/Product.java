package io.github.zeralius.orderflow.inventory.product;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;

@Entity
@Table(name = "products")
public class Product {

  @Id private String sku;

  private String name;

  @Column(precision = 12, scale = 2)
  private BigDecimal price;

  private int availableQuantity;

  @Version private Long version;

  protected Product() {}

  public Product(String sku, String name, BigDecimal price, int availableQuantity) {
    this.sku = sku;
    this.name = name;
    this.price = price;
    this.availableQuantity = availableQuantity;
  }

  public void take(int quantity) {
    if (quantity > availableQuantity) {
      throw new IllegalStateException(
          "Cannot take %d of %s, only %d available".formatted(quantity, sku, availableQuantity));
    }
    availableQuantity -= quantity;
  }

  public void giveBack(int quantity) {
    availableQuantity += quantity;
  }

  public String getSku() {
    return sku;
  }

  public String getName() {
    return name;
  }

  public BigDecimal getPrice() {
    return price;
  }

  public int getAvailableQuantity() {
    return availableQuantity;
  }
}
