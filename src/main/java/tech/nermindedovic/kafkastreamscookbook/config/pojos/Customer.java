package tech.nermindedovic.kafkastreamscookbook.config.pojos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Customer {
    private long customerId;
    private String customerName;
}
