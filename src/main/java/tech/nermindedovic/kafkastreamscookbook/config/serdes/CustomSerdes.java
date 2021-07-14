package tech.nermindedovic.kafkastreamscookbook.config.serdes;

import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import tech.nermindedovic.kafkastreamscookbook.config.pojos.StockEvent;
import tech.nermindedovic.kafkastreamscookbook.config.pojos.WordCount;

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


}
