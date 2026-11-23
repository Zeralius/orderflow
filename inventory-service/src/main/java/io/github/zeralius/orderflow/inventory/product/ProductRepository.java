package io.github.zeralius.orderflow.inventory.product;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

public interface ProductRepository extends JpaRepository<Product, String> {

  /** Row lock until the transaction ends, so two orders can't both take the last item. */
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("select p from Product p where p.sku = :sku")
  Optional<Product> findForUpdate(String sku);
}
