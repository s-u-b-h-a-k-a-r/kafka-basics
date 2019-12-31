package com.kafka.learning.producer;

import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.learning.model.StockPrice;

public class MetricsProducerReporter implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(MetricsProducerReporter.class);
    private final Producer<String, StockPrice> producer;
    private final Set<String> metricsNameFilter = new HashSet<>();

    public MetricsProducerReporter(final Producer<String, StockPrice> producer) {
        this.producer = producer;
        addMetrics();
    }

    private void addMetrics() {
        metricsNameFilter.add("record-queue-time-avg");
        metricsNameFilter.add("record-send-rate");
        metricsNameFilter.add("records-per-request-avg");
        metricsNameFilter.add("request-size-max");
        metricsNameFilter.add("network-io-rate");
        metricsNameFilter.add("record-queue-time-avg");
        metricsNameFilter.add("incoming-byte-rate");
        metricsNameFilter.add("batch-size-avg");
        metricsNameFilter.add("response-rate");
        metricsNameFilter.add("requests-in-flight");
    }

    @Override
    public void run() {
        while (true) {
            final Map<MetricName, ? extends Metric> metrics = producer.metrics();
            displayMetrics(metrics);
            try {
                Thread.sleep(3_000);
            } catch (InterruptedException e) {
                logger.warn("metrics interrupted");
                Thread.interrupted();
                break;
            }
        }
    }

    static class MetricPair {
        private final MetricName metricName;
        private final Metric metric;

        MetricPair(MetricName metricName, Metric metric) {
            this.metricName = metricName;
            this.metric = metric;
        }

        public String toString() {
            return metricName.group() + "." + metricName.name();
        }
    }

    private void displayMetrics(Map<MetricName, ? extends Metric> metrics) {
        final Map<String, MetricPair> metricsDisplayMap = metrics.entrySet().stream()
                // Filter out metrics not in metricsNameFilter
                .filter(metricNameEntry -> metricsNameFilter.contains(metricNameEntry.getKey().name()))
                // Filter out metrics not in metricsNameFilter
                .filter(metricNameEntry -> !Double.isInfinite(metricNameEntry.getValue().value()) && !Double.isNaN(metricNameEntry.getValue().value()) && metricNameEntry.getValue().value() != 0)
                // Turn Map<MetricName,Metric> into TreeMap<String, MetricPair>
                .map(entry -> new MetricPair(entry.getKey(), entry.getValue())).collect(Collectors.toMap(MetricPair::toString, it -> it, (a, b) -> a, TreeMap::new));

        // Output metrics
        final StringBuilder builder = new StringBuilder(255);
        builder.append("\n---------------------------------------\n");
        metricsDisplayMap.entrySet().forEach(entry -> {
            MetricPair metricPair = entry.getValue();
            String name = entry.getKey();
            builder.append(String.format(Locale.US, "%50s%25s\t\t%,-10.2f\t\t%s\n", name, metricPair.metricName.name(), metricPair.metric.value(), metricPair.metricName.description()));
        });
        builder.append("\n---------------------------------------\n");
        logger.info(builder.toString());
    }

}
