package tech.nermindedovic.kafkastreamscookbook.config.serdes;

import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import tech.nermindedovic.kafkastreamscookbook.config.pojos.*;

import java.math.BigDecimal;

public final class CustomSerdes {
    private CustomSerdes () {}

    public static final class StockEventSerde extends Serdes.WrapperSerde<StockEvent> {
        public StockEventSerde() { super(new JsonSerializer<>(), new JsonDeserializer<>(StockEvent.class)); }
    }

    public static final class BigDecimalSerde extends Serdes.WrapperSerde<BigDecimal> {
        public BigDecimalSerde() {super(new JsonSerializer<>(), new JsonDeserializer<>(BigDecimal.class));}
    }

    public static final class WordCountSerde extends Serdes.WrapperSerde<WordCount> {
        public WordCountSerde() {super(new JsonSerializer<>(), new JsonDeserializer<>(WordCount.class));}
    }

    public static final class CustomerSerde extends Serdes.WrapperSerde<Customer> {
        public CustomerSerde() { super(new JsonSerializer<>(), new JsonDeserializer<>(Customer.class)); }
    }

    public static final class ProductSerde extends Serdes.WrapperSerde<Product> {
        public ProductSerde() { super(new JsonSerializer<>(), new JsonDeserializer<>(Product.class));}
    }

    public static final class OrderSerde extends Serdes.WrapperSerde<Order> {
        public OrderSerde() { super(new JsonSerializer<>(), new JsonDeserializer<>(Order.class)); }
    }

    public static final class EnrichedOrderSerde extends Serdes.WrapperSerde<EnrichedOrder> {
        public EnrichedOrderSerde() { super(new JsonSerializer<>(), new JsonDeserializer<>(EnrichedOrder.class)); }
    }

    


}
