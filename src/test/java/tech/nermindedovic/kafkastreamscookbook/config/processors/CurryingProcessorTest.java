package tech.nermindedovic.kafkastreamscookbook.config.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
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
import tech.nermindedovic.kafkastreamscookbook.config.pojos.Customer;
import tech.nermindedovic.kafkastreamscookbook.config.pojos.EnrichedOrder;
import tech.nermindedovic.kafkastreamscookbook.config.pojos.Order;
import tech.nermindedovic.kafkastreamscookbook.config.pojos.Product;
import tech.nermindedovic.kafkastreamscookbook.config.serdes.CustomSerdes;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Slf4j
class CurryingProcessorTest {

    private final CurryingProcessor processor = new CurryingProcessor();

    //serdes
    Serde<Customer> customerSerdes      = new CustomSerdes.CustomerSerde();
    Serde<Product> productSerde         = new CustomSerdes.ProductSerde();
    Serde<Order> orderSerde             = new CustomSerdes.OrderSerde();
    Serde<EnrichedOrder> enrichedOrderSerde = new CustomSerdes.EnrichedOrderSerde();




    private TopologyTestDriver testDriver;
    private TestInputTopic<Long, Customer> customerTableInput;
    private TestInputTopic<Long, Product> productTableInput;
    private TestInputTopic<Long, Order> incomingOrderInput;


    private TestOutputTopic<Long, EnrichedOrder> outputTopic;

    private final Properties properties = new Properties();

    // incoming stream of order
    //custoemrs GlobalTable
    // products global ktable
    // returns a stream of enriched orders

    @BeforeAll
    void setProperties() {
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "currying-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:773");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }



    @AfterEach
    void closeDriver() {
        testDriver.close();
    }

    @Test
    void onNewOrders_willJoinCustomers_andProducts_outputsEnrichedOrder() {
        setupDriver();
        populateGTables();

        List<KeyValue<Long, Order>> orders = getOrders();
        incomingOrderInput.pipeKeyValueList(orders);

        Map<Long, EnrichedOrder> longEnrichedOrderMap = outputTopic.readKeyValuesToMap();
        longEnrichedOrderMap.forEach((k,enriched) -> log.info(k + " : " + enriched + "\n"));
        assertThat(longEnrichedOrderMap).isNotNull();



    }


    void setupDriver() {
        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<Long, Customer> customerGlobalKTable = builder.globalTable("customer-table-input", Consumed.with(Serdes.Long(), customerSerdes));
        GlobalKTable<Long, Product> productGlobalKTable = builder.globalTable("product-table-input", Consumed.with(Serdes.Long(), productSerde));
        KStream<Long, Order> newOrdersStream = builder.stream("incoming-order-input", Consumed.with(Serdes.Long(), orderSerde));


        KStream<Long, EnrichedOrder> apply = processor.enrichOrder().apply(newOrdersStream).apply(customerGlobalKTable).apply(productGlobalKTable);
        apply.to("enrichedOrder-output", Produced.with(Serdes.Long(), enrichedOrderSerde));

        Topology topology = builder.build();
        testDriver = new TopologyTestDriver(topology, properties);

        customerTableInput  = testDriver.createInputTopic("customer-table-input", Serdes.Long().serializer(), customerSerdes.serializer());
        productTableInput   = testDriver.createInputTopic("product-table-input", Serdes.Long().serializer(), productSerde.serializer());
        incomingOrderInput  = testDriver.createInputTopic("incoming-order-input", Serdes.Long().serializer(), orderSerde.serializer());
        outputTopic = testDriver.createOutputTopic("enrichedOrder-output", Serdes.Long().deserializer(), enrichedOrderSerde.deserializer());
    }

    void populateGTables() {
        populateCustomerTable();
        populateProductTable();
    }

    void populateCustomerTable() {
        customerTableInput.pipeInput(100L, new Customer(100L, "Ryan Murray"));
        customerTableInput.pipeInput(101L, new Customer(101L, "Shane Holland"));
        customerTableInput.pipeInput(102L, new Customer(102L, "Chris Smith"));
        customerTableInput.pipeInput(103L, new Customer(103L, "Jason Williams"));
        customerTableInput.pipeInput(104L, new Customer(104L, "Nermin Dedovic"));
        customerTableInput.pipeInput(105L, new Customer(105L, "Vaihbav S"));
    }

    void populateProductTable() {

        productTableInput.pipeInput(100L, new Product(100L, "Product1 - Faucet", new BigDecimal("5.99")));
        productTableInput.pipeInput(101L, new Product(101L, "Product2 - Door", new BigDecimal("45.99")));
        productTableInput.pipeInput(102L, new Product(102L, "Product3 - Basketball", new BigDecimal("29.79")));
        productTableInput.pipeInput(103L, new Product(103L, "Product4 - Coffee Machine", new BigDecimal("75.00")));
        productTableInput.pipeInput(104L, new Product(104L, "Product4 - Screen", BigDecimal.TEN));
        productTableInput.pipeInput(105L, new Product(105L, "Product5 - K2 Keychron", new BigDecimal("45.00")));

    }

    private List<KeyValue<Long, Order>> getOrders() {
        return Arrays.asList(
                new KeyValue<>(1L, new Order(1L, LocalDate.now(), new BigDecimal("45.00"), 105L, 100L )),
                new KeyValue<>(2L,new Order(2L, LocalDate.now(), BigDecimal.TEN, 104L, 101L)),
                new KeyValue<>(3L,new Order(3L, LocalDate.now(), new BigDecimal("75.00"), 103L, 102L)),
                new KeyValue<>(4L,new Order(4L, LocalDate.now(), new BigDecimal("29.79"), 102L, 103L)),
                new KeyValue<>(5L,new Order(5L, LocalDate.now(), new BigDecimal("45.99"), 101L, 104L)),
                new KeyValue<>(6L, new Order(6L, LocalDate.now(), new BigDecimal("5.99"), 100L, 105L))
        );
    }







}