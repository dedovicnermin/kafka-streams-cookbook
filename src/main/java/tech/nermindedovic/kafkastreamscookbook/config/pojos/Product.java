package tech.nermindedovic.kafkastreamscookbook.config.pojos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Product {
    private long productId;
    private String productName;
    private BigDecimal productPrice;


}
