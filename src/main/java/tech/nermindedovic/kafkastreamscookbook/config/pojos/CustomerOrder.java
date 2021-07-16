package tech.nermindedovic.kafkastreamscookbook.config.pojos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class CustomerOrder {
    public Customer customer;
    public Order  order;


    public long productId () {
        return order.getProductId();
    }

}
