package tech.nermindedovic.kafkastreamscookbook.config.processors;



import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tech.nermindedovic.kafkastreamscookbook.config.pojos.*;


import java.util.function.Function;

@Configuration
@Slf4j
public class CurryingProcessor {




    @Bean
    public Function<KStream<Long, Order>,
            Function<GlobalKTable<Long, Customer>,
                    Function<GlobalKTable<Long, Product>, KStream<Long, EnrichedOrder>>>> enrichOrder() {

        return orders -> (
                customers -> (
                        products -> (
                                orders.join(customers,
                                        (orderId, order) -> order.getCustomerId(),
                                        (order, customer) -> new CustomerOrder(customer, order))
                                        .join(products,
                                                (orderId, customerOrder) -> customerOrder
                                                        .productId(),
                                                (customerOrder, product) -> {
                                                    EnrichedOrder enrichedOrder = new EnrichedOrder();
                                                    enrichedOrder.setProduct(product);
                                                    enrichedOrder.setCustomer(customerOrder.customer);
                                                    enrichedOrder.setOrder(customerOrder.order);
                                                    return enrichedOrder;
                                                })
                        )
                )
        );
    }


























}





