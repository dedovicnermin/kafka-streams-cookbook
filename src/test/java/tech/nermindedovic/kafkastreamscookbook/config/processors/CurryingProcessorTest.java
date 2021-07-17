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


        List<KeyValue<Long, EnrichedOrder>> output = outputTopic.readKeyValuesToList();

        List<Customer> customersList = getCustomersList();
        List<Product> productList = getProductList();

        for (int i = 0; i < output.size(); i++) {
            EnrichedOrder enrichedOrder = output.get(i).value;
            assertThat(enrichedOrder.getCustomer()).isEqualTo(customersList.get(i));
            assertThat(enrichedOrder.getProduct()).isEqualTo(productList.get(i));
        }





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
        List<Customer> customers = getCustomersList();
        customerTableInput.pipeInput(100L, customers.get(0));
        customerTableInput.pipeInput(101L, customers.get(1));
        customerTableInput.pipeInput(102L, customers.get(2));
        customerTableInput.pipeInput(103L, customers.get(3));
        customerTableInput.pipeInput(104L, customers.get(4));
        customerTableInput.pipeInput(105L, customers.get(5));
    }


    List<Customer> getCustomersList() {
        return Arrays.asList(
                new Customer(100L, "Ryan Murray"),
                new Customer(101L, "Shane Holland"),
                new Customer(102L, "Chris Smith"),
                new Customer(103L, "Jason Williams"),
                new Customer(104L, "Nermin Dedovic"),
                new Customer(105L, "Vaihbav S")
        );

    }




    void populateProductTable() {

        List<Product> productList = getProductList();
        productTableInput.pipeInput(100L, productList.get(0));
        productTableInput.pipeInput(101L, productList.get(1));
        productTableInput.pipeInput(102L, productList.get(2));
        productTableInput.pipeInput(103L, productList.get(3));
        productTableInput.pipeInput(104L, productList.get(4));
        productTableInput.pipeInput(105L, productList.get(5));

    }


    List<Product> getProductList() {
        return Arrays.asList(
                new Product(100L, "Product1 - Faucet", new BigDecimal("5.99")),
        new Product(101L, "Product2 - Door", new BigDecimal("45.99")),
        new Product(102L, "Product3 - Basketball", new BigDecimal("29.79")),
        new Product(103L, "Product4 - Coffee Machine", new BigDecimal("75.00")),
        new Product(104L, "Product4 - Screen", BigDecimal.TEN),
        new Product(105L, "Product5 - K2 Keychron", new BigDecimal("45.00"))
        );
    }

    private List<KeyValue<Long, Order>> getOrders() {
        return Arrays.asList(
                new KeyValue<>(1L, new Order(1L, LocalDate.now(), new BigDecimal("45.00"), 100L, 100L )),
                new KeyValue<>(2L,new Order(2L, LocalDate.now(), BigDecimal.TEN, 101L, 101L)),
                new KeyValue<>(3L,new Order(3L, LocalDate.now(), new BigDecimal("75.00"), 102L, 102L)),
                new KeyValue<>(4L,new Order(4L, LocalDate.now(), new BigDecimal("29.79"), 103L, 103L)),
                new KeyValue<>(5L,new Order(5L, LocalDate.now(), new BigDecimal("45.99"), 104L, 104L)),
                new KeyValue<>(6L, new Order(6L, LocalDate.now(), new BigDecimal("5.99"), 105L, 105L))
        );
    }







}