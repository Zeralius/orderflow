package io.github.zeralius.orderflow.messaging;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.HashMap;
import java.util.Map;

/**
 * Carries a trace across the outbox. A message is written in one thread (the request or listener)
 * and sent later by the relay in another, so the trace context would normally be lost there. It is
 * stored with the outbox row instead ({@code traceparent} etc.) and picked up again when the relay
 * sends the row, so one order stays one trace from the HTTP request to the last service.
 *
 * <p>Without a tracer (tracing not on the classpath, or switched off) this does nothing.
 */
public class TraceContinuation {

  private final Tracer tracer;
  private final Propagator propagator;

  public TraceContinuation(Tracer tracer, Propagator propagator) {
    this.tracer = tracer;
    this.propagator = propagator;
  }

  /** The current trace context as propagation headers, or empty outside a trace. */
  public Map<String, String> capture() {
    if (tracer == null || propagator == null) {
      return Map.of();
    }
    TraceContext context = tracer.currentTraceContext().context();
    if (context == null) {
      return Map.of();
    }
    Map<String, String> headers = new HashMap<>();
    propagator.inject(context, headers, Map::put);
    return headers;
  }

  /** Runs {@code work} in a span that continues the captured context (a fresh span if empty). */
  public <E extends Exception> void continueWith(
      String spanName, Map<String, String> captured, Work<E> work) throws E {
    if (tracer == null || propagator == null || captured.isEmpty()) {
      work.run();
      return;
    }
    Span span = propagator.extract(captured, Map::get).name(spanName).start();
    try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
      work.run();
    } catch (Exception e) {
      span.error(e);
      throw e;
    } finally {
      span.end();
    }
  }

  @FunctionalInterface
  public interface Work<E extends Exception> {
    void run() throws E;
  }
}
