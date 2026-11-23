package io.github.zeralius.orderflow.inventory;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestSupport.Containers.class)
class InventoryServiceApplicationTests {

  @Test
  void contextLoads() {}
}
