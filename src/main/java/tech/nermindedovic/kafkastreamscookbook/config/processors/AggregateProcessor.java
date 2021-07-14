package tech.nermindedovic.kafkastreamscookbook.config.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tech.nermindedovic.kafkastreamscookbook.config.pojos.StockEvent;
import tech.nermindedovic.kafkastreamscookbook.config.serdes.CustomSerdes;

import java.math.BigDecimal;
import java.util.function.Function;

@Configuration
@Slf4j
public class AggregateProcessor {

    @Bean
    public Function<KStream<String, StockEvent>, KTable<String, BigDecimal>> aggregate() {
        return input -> input
                .peek((k,v) -> log.info(k + " : " + v))
                .groupByKey()
                .aggregate(
                        () -> new BigDecimal("0.00"),
                        (aggKey, newValue, aggValue) -> aggValue.add(newValue.getPrice()),
                        Materialized.<String, BigDecimal, KeyValueStore<Bytes, byte[]>>as("stock-event-aggregate").withKeySerde(Serdes.String()).withValueSerde(new CustomSerdes.BigDecimalSerde())
                );
    }

}
