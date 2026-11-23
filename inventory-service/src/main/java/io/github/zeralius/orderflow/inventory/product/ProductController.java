package io.github.zeralius.orderflow.inventory.product;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.math.BigDecimal;
import java.util.List;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** Read-only view of the catalogue and stock levels. */
@Tag(
    name = "Products",
    description = "The catalogue with prices and current stock. Public, no login needed.")
@RestController
@RequestMapping("/products")
class ProductController {

  private final ProductRepository products;

  ProductController(ProductRepository products) {
    this.products = products;
  }

  @GetMapping
  @Operation(summary = "All products with price and stock")
  List<ProductView> list() {
    return products.findAll(Sort.by("sku")).stream().map(ProductView::from).toList();
  }

  @GetMapping("/{sku}")
  @Operation(summary = "One product by SKU")
  ProductView get(@PathVariable String sku) {
    return products
        .findById(sku)
        .map(ProductView::from)
        .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "No product " + sku));
  }

  record ProductView(String sku, String name, BigDecimal price, int availableQuantity) {
    static ProductView from(Product p) {
      return new ProductView(p.getSku(), p.getName(), p.getPrice(), p.getAvailableQuantity());
    }
  }
}
