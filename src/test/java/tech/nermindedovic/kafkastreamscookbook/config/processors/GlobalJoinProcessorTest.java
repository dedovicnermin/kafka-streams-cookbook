package tech.nermindedovic.kafkastreamscookbook.config.processors;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Slf4j
class GlobalJoinProcessorTest {

    private final GlobalJoinProcessor processor = new GlobalJoinProcessor();
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> clickStreamInput;
    private TestInputTopic<String, String> regionTableInput;
    private TestOutputTopic<String, Long> outputTopic;


    Properties props = new Properties();

    @AfterEach
    void shutdown() {
        testDriver.close();
    }

    @BeforeAll
    void setup() {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testing-globaltable-joins");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:337");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    @Test
    void test() {

        setupTest();
        populateRegionsTable();

        clickStreamInput.pipeInput("nermin", 123L);
        clickStreamInput.pipeInput("ryan", 11L);
        clickStreamInput.pipeInput("vaihbav", 21L);
        clickStreamInput.pipeInput("chris", 15L);
        clickStreamInput.pipeInput("jason", 16L);
        clickStreamInput.pipeInput("nermin", 33L);
        clickStreamInput.pipeInput("nerm", 34L);

        Map<String, Long> map = outputTopic.readKeyValuesToMap();
        log.info(map.toString());

        assertThat(map.get("europe")).isEqualTo(156);
        assertThat(map.get("usa")).isEqualTo(11 + 15);
        assertThat(map.get("east-asia")).isEqualTo(21);
        assertThat(map.get("UNKNOWN")).isEqualTo(34);
        assertThat(map.get("africa")).isEqualTo(16);





    }

    void setupTest() {
        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, String> regionsTable = builder.globalTable("gTable-input-topic", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, Long> clicksStream = builder.stream("clicks-input-topic", Consumed.with(Serdes.String(), Serdes.Long()));

        KStream<String, Long> apply = processor.process().apply(clicksStream, regionsTable);
        apply.to("streamGTable-join-output-topic", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();

        testDriver = new TopologyTestDriver(topology, props);
        clickStreamInput = testDriver.createInputTopic("clicks-input-topic", Serdes.String().serializer(), Serdes.Long().serializer());
        regionTableInput = testDriver.createInputTopic("gTable-input-topic", Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = testDriver.createOutputTopic("streamGTable-join-output-topic", Serdes.String().deserializer(), Serdes.Long().deserializer());


    }

    void populateRegionsTable() {
        List<KeyValue<String, String>> inputs = Arrays.asList(
                new KeyValue<>("nermin", "europe"),
                new KeyValue<>("jason", "africa"),
                new KeyValue<>("ryan", "west-asia"),
                new KeyValue<>("chris", "usa"),
                new KeyValue<>("vaihbav", "east-asia"),
                new KeyValue<>("ryan", "usa")
        );

        regionTableInput.pipeKeyValueList(inputs);

    }







}