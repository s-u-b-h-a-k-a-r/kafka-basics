package com.kafka.learning.producer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.learning.StockAppConstants;
import com.kafka.learning.model.StockPrice;

public class StockPriceKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(StockPriceKafkaProducer.class);

    private static Producer<String, StockPrice> createProducer() {
        final Properties props = new Properties();
        setupBootstrapAndSerializers(props);
        setupBatchingAndCompression(props);
        setupRetriesInFlightTimeout(props);

        // Set number of acknowledgments - acks - default is all
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Install interceptor list - config "interceptor.classes"
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, StockProducerInterceptor.class.getName());

        props.put("importantStocks", "IBM,UBER");

        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, StockPricePartitioner.class.getName());

        return new KafkaProducer<>(props);
    }

    private static void setupRetriesInFlightTimeout(Properties props) {
        // Only two in-flight messages per Kafka broker connection

        // - max.in.flight.requests.per.connection (default 5)
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        // Set the number of retries - retries
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Request timeout - request.timeout.ms
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);

        // Only retry after one second.
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1_000);
    }

    private static void setupBootstrapAndSerializers(Properties props) {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, StockAppConstants.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "StockPriceKafkaProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StockPriceSerializer.class.getName());

    }

    private static void setupBatchingAndCompression(final Properties props) {

        // Linger up to 100 ms before sending batch if size not met
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);

        // Batch up to 64K buffer sizes.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384 * 4);

        // Use Snappy compression for batch compression.
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    }

    public static void main(String... args) throws Exception {
        final Producer<String, StockPrice> producer = createProducer();
        final List<StockSender> stockSenders = getStockSenderList(producer);
        final ExecutorService executorService = Executors.newFixedThreadPool(stockSenders.size() + 1);
        executorService.submit(new MetricsProducerReporter(producer));
        stockSenders.forEach(executorService::submit);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executorService.shutdown();
            try {
                executorService.awaitTermination(200, TimeUnit.MILLISECONDS);
                logger.info("Flushing and closing producer");
                producer.flush();
                producer.close(Duration.ofMillis(10_000));
            } catch (InterruptedException e) {
                logger.warn("shutting down", e);
            }
        }));
    }

    private static List<StockSender> getStockSenderList(final Producer<String, StockPrice> producer) {
        List<StockSender> senders = new ArrayList<>();
        senders.add(new StockSender(StockAppConstants.TOPIC, new StockPrice("IBM", 100, 99), new StockPrice("IBM", 50, 10), producer, 1, 10));
        senders.add(new StockSender(StockAppConstants.TOPIC, new StockPrice("SUN", 100, 99), new StockPrice("SUN", 50, 10), producer, 1, 10));
        senders.add(new StockSender(StockAppConstants.TOPIC, new StockPrice("GOOG", 500, 99), new StockPrice("GOOG", 400, 10), producer, 1, 10));
        senders.add(new StockSender(StockAppConstants.TOPIC, new StockPrice("INEL", 100, 99), new StockPrice("INEL", 50, 10), producer, 1, 10));
        senders.add(new StockSender(StockAppConstants.TOPIC, new StockPrice("UBER", 1000, 99), new StockPrice("UBER", 50, 0), producer, 1, 10));
        senders.add(new StockSender(StockAppConstants.TOPIC, new StockPrice("ABC", 100, 99), new StockPrice("ABC", 50, 10), producer, 1, 10));
        senders.add(new StockSender(StockAppConstants.TOPIC, new StockPrice("XYZ", 100, 99), new StockPrice("XYZ", 50, 10), producer, 1, 10));
        senders.add(new StockSender(StockAppConstants.TOPIC, new StockPrice("DEF", 100, 99), new StockPrice("DEF", 50, 10), producer, 1, 10));
        senders.add(new StockSender(StockAppConstants.TOPIC, new StockPrice("DEF", 100, 99), new StockPrice("DEF", 50, 10), producer, 1, 10));
        senders.add(new StockSender(StockAppConstants.TOPIC, new StockPrice("AAA", 100, 99), new StockPrice("AAA", 50, 10), producer, 1, 10));
        senders.add(new StockSender(StockAppConstants.TOPIC, new StockPrice("BBB", 100, 99), new StockPrice("BBB", 50, 10), producer, 1, 10));
        senders.add(new StockSender(StockAppConstants.TOPIC, new StockPrice("CCC", 100, 99), new StockPrice("CCC", 50, 10), producer, 1, 10));
        senders.add(new StockSender(StockAppConstants.TOPIC, new StockPrice("DDD", 100, 99), new StockPrice("DDD", 50, 10), producer, 1, 10));
        senders.add(new StockSender(StockAppConstants.TOPIC, new StockPrice("EEE", 100, 99), new StockPrice("EEE", 50, 10), producer, 1, 10));
        senders.add(new StockSender(StockAppConstants.TOPIC, new StockPrice("FFF", 100, 99), new StockPrice("FFF", 50, 10), producer, 1, 10));
        return senders;
    }
}
