package tech.nermindedovic.kafkastreamscookbook.config.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.BiFunction;

@Configuration
@Slf4j
public class GlobalJoinProcessor {

    @Bean
    public BiFunction<KStream<String, Long>, GlobalKTable<String, String>, KStream<String, Long>> process() {
        return (userClicksStream, userRegionsTable) -> userClicksStream
                .leftJoin(
                        userRegionsTable,
                        (leftKey_name,leftValue) -> leftKey_name,                                                                              /* derive a (potentially) new key by which to lookup agains the table */
                        (leftValueClicks, rightValueRegion) -> new RegionWithClicks(rightValueRegion == null ? "UNKNOWN" : rightValueRegion, leftValueClicks)                   /* ValueJoiner */
                )
                .map((user, regionWithClicks) -> new KeyValue<>(regionWithClicks.getRegion(), regionWithClicks.getClicks()))
                .peek((k, v) -> log.info(k + " : " + v))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .reduce(Long::sum)
                .toStream();
    }
    //.reduce((firstClicks, secondClicks) -> firstClicks + secondClicks)


    private static final class RegionWithClicks {

        private final String region;
        private final long clicks;

        public RegionWithClicks(String region, long clicks) {
            if (region == null || region.isEmpty()) {
                throw new IllegalArgumentException("region must be set");
            }
            if (clicks < 0) {
                throw new IllegalArgumentException("clicks must not be negative");
            }
            this.region = region;
            this.clicks = clicks;
        }

        public String getRegion() {
            return region;
        }

        public long getClicks() {
            return clicks;
        }

    }

}
