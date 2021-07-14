package tech.nermindedovic.kafkastreamscookbook.config.processors;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.jupiter.api.*;
import tech.nermindedovic.kafkastreamscookbook.config.pojos.StockEvent;
import tech.nermindedovic.kafkastreamscookbook.config.serdes.CustomSerdes;


import java.math.BigDecimal;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AggregateProcessorTest {

    static final String SYM = "SYM";
    static final String AAPL = "AAPL";

    Serde<String> stringSerde = Serdes.String();
    Serde<StockEvent> stockEventSerde = new CustomSerdes.StockEventSerde();
    Serde<BigDecimal> bigDecimalSerde = new CustomSerdes.BigDecimalSerde();

    private TopologyTestDriver testDriver;

    private TestInputTopic<String, StockEvent> inputTopic;
    private TestOutputTopic<String, BigDecimal> outputTopic;

    Properties props = new Properties();

    final AggregateProcessor aggregateProcessor = new AggregateProcessor();

    @BeforeAll
    void setup() {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-aggregate-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:773");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass());
    }

    @AfterEach
    void close() {
        testDriver.close();
    }


    @Test
    void whenGivenStockEvent_willAggregatePrice() {
        setAggregateProcessor();
        final List<StockEvent> stockEvents = Arrays.asList(
                new StockEvent(SYM, BigDecimal.TEN),
                new StockEvent(SYM, BigDecimal.TEN),
                new StockEvent(SYM, new BigDecimal("5.99")),
                new StockEvent(AAPL, new BigDecimal("1.51")),
                new StockEvent(AAPL, new BigDecimal("2.99")),
                new StockEvent(AAPL, new BigDecimal("8.45"))
        );

        LinkedList<KeyValue<String, StockEvent>> input = new LinkedList<>();
        for (final StockEvent event : stockEvents) {
            input.add(new KeyValue<>(event.getSymbol(), event));
        }

        inputTopic.pipeKeyValueList(input);

        Map<String, BigDecimal> outputMap = outputTopic.readKeyValuesToMap();
        Assertions.assertAll(
                () -> assertThat(outputMap).hasSize(2),
                () -> assertThat(outputMap).containsEntry(SYM, new BigDecimal("25.99")),
                () -> assertThat(outputMap).containsEntry(AAPL, new BigDecimal("12.95"))
        );





    }

    private void setAggregateProcessor() {
        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, StockEvent> stream = builder.stream("aggregate-input-topic", Consumed.with(stringSerde, stockEventSerde));
        KTable<String, BigDecimal> apply = aggregateProcessor.aggregate().apply(stream);
        apply.toStream().to("aggregate-output-topic");
        final Topology topology = builder.build();

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic("aggregate-input-topic", stringSerde.serializer(), stockEventSerde.serializer());
        outputTopic = testDriver.createOutputTopic("aggregate-output-topic", stringSerde.deserializer(), bigDecimalSerde.deserializer());
    }







}