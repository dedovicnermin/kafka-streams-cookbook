package tech.nermindedovic.kafkastreamscookbook.config.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tech.nermindedovic.kafkastreamscookbook.config.pojos.WordCount;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.function.Function;

@Configuration
@Slf4j
public class CountProcessor {

    @Bean
    Function<KStream<Bytes, String>, KStream<String, WordCount>> processWords () {
        return input -> input
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .map((key, value) -> new KeyValue<>(value, value))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(30)))
                .count(Materialized.as("wordcounts"))
                .toStream()
                .map((k,v) -> new KeyValue<>(k.key(), new WordCount(k.key(), v.intValue(), new Date(k.window().start()), new Date(k.window().end()))))
                .peek((k,v) -> log.info(k + " : " + v))
                ;
    }

}
