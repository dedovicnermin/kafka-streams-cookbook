package tech.nermindedovic.kafkastreamscookbook.config.pojos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Order {
    private long orderId;
    private LocalDate orderDate;
    private BigDecimal orderTotal;
    private long productId;
    private long customerId;




}