package tech.nermindedovic.kafkastreamscookbook.service;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class ServiceAPI {

    private final QueryService service;

    @GetMapping("/aggregate/all")
    public String getAggregateEvents() {
        return service.getAggregateEvents();
    }

    @GetMapping("/aggregate/{symbol}")
    public String getAggregateValue(@PathVariable String symbol) {
        return service.getValueForAggregate(symbol);
    }





}
