package tech.nermindedovic.kafkastreamscookbook.config.processors;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import tech.nermindedovic.kafkastreamscookbook.config.pojos.WordCount;
import tech.nermindedovic.kafkastreamscookbook.config.serdes.CustomSerdes;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CountProcessorTest {

    private final CountProcessor processor = new CountProcessor();

    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<Bytes> bytesSerde = Serdes.Bytes();
    private final Serde<WordCount> wordCountSerde = new CustomSerdes.WordCountSerde();


    private TopologyTestDriver testDriver;
    private TestInputTopic<Bytes, String> inputTopic;
    private TestOutputTopic<String, WordCount> outputTopic;

    private Properties props = new Properties();



    @BeforeAll
    void setup() {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-aggregate-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:773");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass());
    }

    @AfterEach
    void shutdown() {
        testDriver.close();
    }


    @Test
    void countsWordsAccurately() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Bytes, String> stream = builder.stream("count-input-topic", Consumed.with(Serdes.Bytes(), stringSerde));
        KStream<String, WordCount> apply = processor.processWords().apply(stream);
        apply.to("count-output-topic", Produced.with(stringSerde, wordCountSerde));

        final Topology topology = builder.build();
        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic("count-input-topic", bytesSerde.serializer(), stringSerde.serializer());
        outputTopic = testDriver.createOutputTopic("count-output-topic", stringSerde.deserializer(), wordCountSerde.deserializer());


        inputTopic.pipeInput(null,"dream DREAM dReAm");
        final Map<String, WordCount> map = outputTopic.readKeyValuesToMap();
        WordCount wordCount = map.get("dream");
        assertThat(wordCount.getWord()).isEqualTo("dream");
        assertThat(wordCount.getCount()).isEqualTo(3);

    }



}