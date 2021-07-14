package tech.nermindedovic.kafkastreamscookbook.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
@RequiredArgsConstructor
public class QueryService {

    private final InteractiveQueryService service;

    public String getAggregateEvents() {
        StringBuilder builder = new StringBuilder();
        final ReadOnlyKeyValueStore<String, BigDecimal> queryableStore = service.getQueryableStore("stock-event-aggregate", QueryableStoreTypes.keyValueStore());
        queryableStore.all().forEachRemaining(
                stringBigDecimalKeyValue -> builder.append(stringBigDecimalKeyValue.key)
                        .append(" : ")
                        .append(stringBigDecimalKeyValue.value)
                        .append("\n")
        );
        return builder.toString();
    }

    public String getValueForAggregate(String symbol) {
        final ReadOnlyKeyValueStore<String, BigDecimal> queryableStore = service.getQueryableStore("stock-event-aggregate", QueryableStoreTypes.keyValueStore());
        return symbol + " : " + queryableStore.get(symbol);
    }


}
