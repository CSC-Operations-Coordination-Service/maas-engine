# Replay worker

`python -m maas_engine.replay --replay-config replay.json` (or `maas_replay`) runs the
replays an operator asks for. It is the pattern behind the download-manager's recollect
and the maas-collector's operator, once:

1. a backend writes an **operation record** (status `PUBLISHED`) in OpenSearch, then
   publishes a **replay request** on RabbitMQ;
2. a **lane** of the worker takes the request, acknowledges it, closes its connection
   and runs a **child process** under a deadline;
3. while the child runs, the worker saves the last 20 lines of its output into the
   record every 30 s (`RUNNING`), then sets `DONE` (exit code 0) or `FAILED` with the
   exit code and the output tail.

The worker knows nothing about what it replays. Where the configuration comes from and
which command replays an interface are the job of a **handler** you provide.

## Why not an engine

An engine holds a message until its handler returns, with an AMQP heartbeat of
`AMQP_HEARTBEAT` (600 s by default), and RabbitMQ closes a channel whose delivery stays
unacknowledged for `consumer_timeout` (30 minutes by default). A replay runs for hours.
So a lane **polls** its queue with `basic_get`, **acknowledges the request on receipt**,
closes the connection, and runs the child. The consequences:

- a replay is never redelivered, which would collect the window twice;
- a request lost to a crash keeps its record `RUNNING`: the lost replay is the one whose
  `updated_at` stopped moving;
- there is no AMQP connection to miss a heartbeat while the replay runs.

A lane takes one request at a time. Lanes run side by side, each in a thread.

## Configuration

```json
{
  "exchange": {"name": "maas-operator-exchange", "type": "topic"},
  "operation_index": "maas-operation",
  "create_missing_records": false,
  "workdir": "/tmp",
  "poll_interval": 5,
  "progress_interval": 30,
  "handler": {"class": "package.module:Class", "kwargs": {"service": "odata"}},
  "lanes": [
    {"name": "short", "queue": "maas-operator-odata-replay-short",
     "routing_key": "maas.operator.odata.replay.short", "timeout": {"base": 21600}},
    {"name": "long", "queue": "maas-operator-odata-replay-long",
     "routing_key": "maas.operator.odata.replay.long",
     "timeout": {"base": 21600, "per_day": 10800}}
  ]
}
```

| key | meaning |
| --- | --- |
| `exchange` | the durable exchange the queues are bound to (`type` defaults to `topic`) |
| `operation_index` | index or alias of the operation records |
| `create_missing_records` | create a record the backend did not write; default is to log an error and go on |
| `workdir` | folder handed to the handler (default: the system temp folder) |
| `poll_interval` | seconds between two polls of an empty queue |
| `progress_interval` | seconds between two saves of the output tail |
| `handler` | `class` as `package.module:Class`, built with `kwargs` |
| `lanes[].timeout` | seconds a replay may run: `base`, or `per_day` per day of its window when that is more. The queue the request came from decides, not the `lane` field of the body |

Keys starting with `_` at the top level are ignored (comments). Anywhere else an unknown
key is an error, and the worker stops at startup with what is wrong.

The connections come from the engine's options and environment: `ES_URL`,
`ES_USERNAME`, `ES_PASSWORD` or `ES_VAULT_PATH`, `AMQP_URL` or `AMQP_VAULT_PATH`, the
`VAULT_*` settings (see [vault.md](vault.md)), `--healthcheck-port` /
`HEALTHCHECK_PORT` (0 for no health check) and `-v` / `-vv`. `REPLAY_CONFIG` can replace
`--replay-config`.

## The request

A JSON object. The queue, its routing key and the exchange are in the configuration:
the worker does not impose names.

```json
{"operation_id": "…", "interface_name": "PRIP_S3A_ACRI",
 "start_date": "2026-10-01T00:00:00Z", "end_date": "2026-10-02T00:00:00Z",
 "requested_by": "bob", "lane": "short"}
```

`operation_id` is the id of the operation record. Dates are ISO 8601; a date without an
offset is UTC. `start_date` must be before `end_date`. Any other field is passed to the
handler in `request.extra`. A body that does not parse is dropped, and its record set to
`FAILED` when it carries an `operation_id`.

## The handler

```python
from maas_engine.replay import ReplayNotPossible


class MyHandler:
    def __init__(self, service):  # the kwargs of the configuration
        self.service = service

    def prepare(self, request, lane, workdir):
        """check the request, make what the command needs, return a token"""
        if request.interface_name not in self.known():
            raise ReplayNotPossible(f"{request.interface_name} is not configured")
        return tempfile.mkdtemp(dir=workdir)

    def command(self, prepared, request, lane):
        """-> (argv, environment) of the child"""
        return ["python", "-m", "my.replay", "--out", prepared], dict(os.environ)

    def cleanup(self, prepared):
        shutil.rmtree(prepared, ignore_errors=True)

    def describe_failure(self, code, tail):  # optional
        return f"my replay failed with {code}\n{tail}"
```

`prepare` raises `ReplayNotPossible` for a request that cannot run; the message goes into
the record. It removes what it made before it raises: `cleanup` only follows a successful
`prepare`. A handler shared by the lanes is called from several threads.

If a handler passes the operation id on a command line, mind that an id may start with a
dash: use `--flag=value`, not two arguments.

## The record

The worker only updates: `status`, `updated_at`, and when known `exit_code` and `details`
(the output tail, or the reason). It never raises over a record: a failed update is
logged and the replay goes on.

| status | by | when |
| --- | --- | --- |
| `PUBLISHED` | the backend | before it publishes the request |
| `RUNNING` | the worker | when it starts, then every `progress_interval` with the output tail |
| `DONE` | the worker | the child exited 0 |
| `FAILED` | the worker | exit code not 0, `ReplayNotPossible`, a malformed request, a timeout (exit code -9) or a stop (-15) |

## Stopping

SIGTERM and SIGINT stop the worker: the children get SIGTERM, then are killed after 10 s,
and the records say so. A request taken after the stop is failed, not run.

## Health check

`GET /` answers 200 while every lane thread runs, and 500 when one has died. The worker
holds no AMQP connection between polls, so there is nothing else to probe.

## Tests

`tests/test_replay_*.py`: the worker runs on kombu's in-memory transport with real child
processes, so the deadline, terminate and the acknowledgement on receipt are tested for
real. They need neither RabbitMQ nor OpenSearch.
