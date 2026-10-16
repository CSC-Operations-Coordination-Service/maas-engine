# Telemetry

The engines can send OpenTelemetry traces and metrics over OTLP, through the
`maas_model.telemetry` library. Telemetry is **off by default**: the engine then
behaves, logs, acknowledges and publishes exactly as without it, and `opentelemetry`
is not even imported.

The library itself, its environment variables and its rules are described in the
maas-model documentation, `docs/telemetry.md` of the maas-model repository.

## Enabling it

Install the extra, which pulls `maas-model[telemetry]` and the OpenTelemetry SDK and
OTLP/HTTP exporter:

```console
pip install "maas-engine[telemetry]"
```

Then turn it on with `--telemetry`, or with `MAAS_TELEMETRY` set to `1`, `true`, `yes`
or `on`. Both `python -m maas_engine` and `maas_publish_message` take the option.
Without the extra, enabling telemetry logs one warning and the engine runs as before.

Where the data goes and who sends it come from the environment, the same variables as
the collectors. In a docker swarm service:

```yaml
environment:
  MAAS_TELEMETRY: "1"
  OTEL_EXPORTER_OTLP_ENDPOINT: http://alloy:4318
  OTEL_EXPORTER_OTLP_PROTOCOL: http/protobuf
  MAAS_DEPLOYMENT: "{{.Service.Name}}"
  MAAS_INSTANCE_ID: "{{.Service.Name}}.{{.Task.Slot}}"
```

| Variable | Effect |
| --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP receiver, the Alloy agent of the stack |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf`, the only protocol the extra installs |
| `MAAS_DEPLOYMENT` | the `deployment` label of every metric, the swarm service name. Default: the host name |
| `MAAS_INSTANCE_ID` | the `instance` label and `service.instance.id`, one per replica. Default: `<hostname>-<pid>` |

The resource is `service.name=maas-engine`, `service.namespace=maas` and
`maas.component=engine`. Mimir turns it into `job="maas/maas-engine"` and
`instance=<MAAS_INSTANCE_ID>`.

## What is emitted

### Traces

The `process` span continues the trace of the message's producer: the trace context
travels in the AMQP message **headers** (`traceparent`, `tracestate`, `baggage`), never
in the JSON body, which the engine still validates strictly. A file collected by a
collector, its message and the engine runs it triggers are therefore one trace.

| Span | Parent | Attributes |
| --- | --- | --- |
| `process <queue>` | the producer's `publish` span | `messaging.*`, `maas.message_id`, `maas.document.count`, `maas.document.class`, `maas.pipeline`, `maas.message.redelivered`, `maas.message.outcome`, `maas.message.reject_reason`, and `maas.origin.interface` from the baggage |
| `opensearch.connect` | `process` | |
| `engine.run <ENGINE_ID>` | `process` | `maas.engine.id`, `maas.engine.class`, `maas.documents.input`, `maas.reports.count` |
| `engine.load_input` | `engine.run` | `maas.documents.missing` when input documents are missing |
| `engine.bulk` | `engine.run` | |
| `opensearch <operation>` | the span current when the request is made; the bulk requests of the `parallel_bulk` threads hang from `engine.bulk` | `db.operation.name`, `db.collection.name`, `http.response.status_code` |
| `publish <exchange>` | `process` | `messaging.*`, `maas.message_id`, `maas.document.count`, `maas.document.class` |

`<queue>` is the configured queue bound to the delivery's exchange and routing key, or
empty when none matches.

### Outcome and reason of a message

Each branch of the message handling sets the `outcome` and `reason` of the `process`
span and of the `maas_messages_consumed_total` and
`maas_message_processing_duration_seconds` metrics:

| Branch | outcome | reason |
| --- | --- | --- |
| invalid body | reject | `invalid_body` |
| engine stopping | requeue | `stopping` (left unacknowledged, redelivered) |
| start failure (OpenSearch connection) | requeue | `start_failed` |
| `HandleMessageException` | requeue | `retryable` |
| OpenSearch error or `ImproperlyConfigured` | requeue | `opensearch` |
| `CannotProcessMessageException` | reject | `normal` |
| any other exception | reject | `abnormal` |
| success | ack | `normal` |

An exception escaping the success branch keeps the library default, `error` /
`exception`. There is no dead-letter exchange: a `reject` drops the message.

### Metrics

The Prometheus names, as Mimir stores them. Every series also carries `component`,
`deployment` and `instance`.

| Metric | Labels | Meaning |
| --- | --- | --- |
| `maas_messages_consumed_total` | exchange, routing_key, queue, outcome, reason, redelivered | messages handled |
| `maas_message_processing_duration_seconds_*` | queue, outcome | time to handle one message |
| `maas_message_documents_bucket` | direction, exchange, routing_key | documents per consumed (`in`) and published (`out`) message |
| `maas_engine_run_duration_seconds_*` | node, routing_key, outcome | one engine run, `ok` or `error` |
| `maas_engine_documents_total` | node, result | documents written: `created`, `updated`, `deleted`, `error`, `conflict` |
| `maas_engine_input_documents_total` | node, document_class | input documents asked for by the messages |
| `maas_engine_input_missing_total` | node, document_class | input documents not found |
| `maas_engine_reports_documents_total` | node, exchange, routing_key | documents reported on the bus |
| `maas_messages_published_total` | exchange, routing_key, outcome | reports published, `ok` or `error` |
| `maas_messages_returned_total` | exchange, routing_key, reason | reports the broker returned, `NO_ROUTE` or `other` |
| `maas_opensearch_time_seconds_total`, `maas_opensearch_requests_total` | node_type, node, operation, index | OpenSearch time and requests per engine, including the `parallel_bulk` threads |
| `maas_opensearch_request_duration_seconds_*` | operation, index, status_class | OpenSearch request duration |

`node` is the `ENGINE_ID`. Message and document ids are span attributes only, never
labels.

`maas_engine_input_documents_total` and `maas_engine_input_missing_total` count only for
the engine that loads the input documents of the session, the first data engine to run
for a message. The other data engines of the same message reuse the session's input
documents and add nothing to them.

### Topology

At start-up, the engine declares its part of the flow map as info gauges, from its
configuration:

- `maas_node_info{node_type="engine"}`: one node per engine id of the `amqp` events,
  with the engine class as `impl`.
- `maas_topology_route_info{node_type="engine", direction="in", source="config"}`: one
  route per queue and engine, with the queue's `exchange`, `routing_key` and `queue`.
- `maas_topology_route_info{node_type="engine", direction="out", source="config"}`:
  the routes the engine class declares in `OUTPUT_ROUTES`, optional:

  ```python
  class ComputeXEngine(DataEngine):
      ENGINE_ID = "COMPUTE_X"
      OUTPUT_ROUTES = (("etl-exchange", "new.compute-x"),)
  ```

At run time, every report the engine publishes declares its route as
`direction="out", source="observed"`, with its `document_class`. An engine therefore
appears in the flow map with its out-routes once it has published, and before that if it
declares `OUTPUT_ROUTES`.

`maas_build_info` gives the maas-engine and maas-model versions.

## Replays

`maas_publish_message` publishes a folder of payloads with the header
`maas.replay=true`, whether telemetry is on or off. With telemetry, each message gets a
`publish <exchange>` span with the attribute `maas.replay=true`, which starts the trace
of the replayed message.

A replay has its own telemetry instance. It is usually run with `docker exec` inside an
engine container, whose `MAAS_TELEMETRY`, `MAAS_DEPLOYMENT` and `MAAS_INSTANCE_ID` are
the live engine's. If the replay pushed under the same `component`, `deployment` and
`instance`, its cumulative series would collide with the engine's. `maas_publish_message`
therefore sets `service.instance.id`, the `instance` label, to
`<MAAS_INSTANCE_ID>-replay-<pid>`, for example `engine-s1.1-replay-4242`. When
`MAAS_INSTANCE_ID` is not set, the host name stands in for it. The replay keeps the
engine's `deployment`, so dashboards still find its series, under their own instance.
This follows the convention of the maas-collector replays,
`<instance id of the worker>-replay-<lane>`.

## The statistics line

The statistics log line of a data engine is unchanged:

```text
INFO:<EngineClass>: 1 created, 0 updated, 0 deleted, 0 errors, 0 conflicts in 0.025s
```

`maas_engine_stats` keeps parsing it. The same counts are in
`maas_engine_documents_total`.
