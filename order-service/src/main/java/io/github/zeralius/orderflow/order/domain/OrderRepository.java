package io.github.zeralius.orderflow.order.domain;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, UUID> {

  List<Order> findByCustomerIdOrderByCreatedAtDesc(String customerId);
}
