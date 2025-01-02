/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib;

import static io.opentelemetry.api.GlobalOpenTelemetry.resetForTest;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static io.opentelemetry.semconv.resource.attributes.ResourceAttributes.SERVICE_NAME;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of the {@link MetricClient} that sends the provided metric data to an
 * OpenTelemetry compliant metrics store.
 * <p>
 * Any {@link MetricAttribute}s provided along with the metric data are passed as key/value pairs
 * annotating the metric.
 */
public class OpenTelemetryMetricClient implements MetricClient {

  private Meter meter;
  private SdkMeterProvider meterProvider;

  private final Map<String, ObservableDoubleGauge> gauges = new HashMap<>();
  private final Map<String, Map<Attributes, Double>> gaugeValues = Collections.synchronizedMap(new HashMap<>());

  @Override
  public void count(final MetricsRegistry metric, final long val, final MetricAttribute... attributes) {
    final LongCounter counter = meter
        .counterBuilder(metric.getMetricName())
        .setDescription(metric.getMetricDescription())
        .build();

    final AttributesBuilder attributesBuilder = buildAttributes(attributes);
    counter.add(val, attributesBuilder.build());
  }

  @Override
  public void gauge(final MetricsRegistry metric, final double val, final MetricAttribute... attributes) {
    /*
     * The Gauge builder in the OpenTelemetry Java SDK can only collect gauge values asynchronously via
     * a callback.
     *
     * This implementation uses the sync map to ensure gauges are defined only once. It creates a
     * synchronized map which can be updated by subsequent calls without redefining the gauge.
     *
     * This sort-of a hack: OpenTelemetry expects you to define your gauge up-front and provide a
     * callback that the SDK will call periodically. However, this API does not conform to the
     * MetricClient interface. Without some refactoring of the client interface, this adapter is
     * necessary.
     */
    final Attributes attr = buildAttributes(attributes).build();
    final String name = metric.getMetricName();
    synchronized (gauges) { // sync so we don't create the same gauge concurrently
      if (!gauges.containsKey(name)) {
        // create an in-memory sync map for reading the latest value given the attribute set
        final var valueMap = Collections.synchronizedMap(new HashMap<Attributes, Double>());
        gaugeValues.put(name, valueMap); // Register this in-memory map with this gauge
        valueMap.put(attr, val); // Must insert the initial value so the callback will see it on its first poll

        // Build the gauge with a callback that reads from the sync map to get the current values for each
        // attribute set
        // The OpenTelemetry SDK will call this periodically to read the current values.
        final var gauge = meter.gaugeBuilder(name).setDescription(metric.getMetricDescription()).buildWithCallback(measurement -> {
          for (final Map.Entry<Attributes, Double> entry : valueMap.entrySet()) {
            measurement.record(entry.getValue(), entry.getKey());
          }
        });
        gauges.put(name, gauge);
        return;
      }
    }
    // This is outside the sync block since we don't need to create a new gauge/gaugeValues map
    // and at this point they are both guaranteed to exist.
    final var valueMap = gaugeValues.get(name);
    valueMap.put(attr, val);
  }

  @Override
  public void distribution(final MetricsRegistry metric, final double val, final MetricAttribute... attributes) {
    final DoubleHistogram histogramMeter = meter.histogramBuilder(metric.getMetricName()).setDescription(metric.getMetricDescription()).build();
    final AttributesBuilder attributesBuilder = buildAttributes(attributes);
    histogramMeter.record(val, attributesBuilder.build());
  }

  /**
   * Initialize client.
   *
   * @param metricEmittingApp means of understanding where metrics are being emitted from
   * @param otelEndpoint where metrics will be sent to
   */
  public void initialize(final MetricEmittingApp metricEmittingApp, final String otelEndpoint) {
    final Resource resource = Resource.getDefault().toBuilder().put(SERVICE_NAME, metricEmittingApp.getApplicationName()).build();

    final SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
        .addSpanProcessor(
            BatchSpanProcessor
                .builder(OtlpGrpcSpanExporter.builder().setEndpoint(otelEndpoint).build())
                .build())
        .setResource(resource)
        .build();
    final MetricExporter metricExporter = OtlpGrpcMetricExporter.builder()
        .setEndpoint(otelEndpoint).build();
    initialize(metricEmittingApp, metricExporter, sdkTracerProvider, resource);
  }

  @VisibleForTesting
  void initialize(
                  final MetricEmittingApp metricEmittingApp,
                  final MetricExporter metricExporter,
                  final SdkTracerProvider sdkTracerProvider,
                  final Resource resource) {
    meterProvider = SdkMeterProvider.builder()
        .registerMetricReader(PeriodicMetricReader.builder(metricExporter).build())
        .setResource(resource)
        .build();

    final OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
        .setTracerProvider(sdkTracerProvider)
        .setMeterProvider(meterProvider)
        .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
        .buildAndRegisterGlobal();

    meter = openTelemetry.meterBuilder(metricEmittingApp.getApplicationName())
        .build();
  }

  @VisibleForTesting
  SdkMeterProvider getSdkMeterProvider() {
    return meterProvider;
  }

  @Override
  public void shutdown() {
    resetForTest();
    closeGauges();
  }

  private void closeGauges() {
    synchronized (gauges) {
      for (final Map.Entry<String, ObservableDoubleGauge> entry : gauges.entrySet()) {
        entry.getValue().close();
      }
      gauges.clear();
      gaugeValues.clear();
    }
  }

  private AttributesBuilder buildAttributes(final MetricAttribute... attributes) {
    final AttributesBuilder attributesBuilder = Attributes.builder();
    for (final MetricAttribute attribute : attributes) {
      attributesBuilder.put(stringKey(attribute.key()), attribute.value());
    }
    return attributesBuilder;
  }

}
