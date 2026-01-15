from prometheus_client import Counter, Histogram, Gauge

REQ_LATENCY = Histogram(
    "service_request_latency_seconds",
    "Request latency in seconds",
    ["service", "endpoint", "method", "status"],
    buckets=(0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10),
)

REQ_COUNT = Counter(
    "service_requests_total",
    "Total requests",
    ["service", "endpoint", "method", "status"],
)

TX_COUNT = Counter(
    "distributed_transactions_total",
    "Distributed transaction decisions",
    ["service", "protocol", "decision"],
)

LEADER_GAUGE = Gauge(
    "service_is_leader",
    "Whether this instance considers itself leader",
    ["service", "instance_id"],
)

EVENT_COUNT = Counter(
    "events_total",
    "Events produced or consumed",
    ["service", "stream", "direction"],
)

QUEUE_DELAY = Histogram(
    "scheduler_queue_delay_seconds",
    "Queueing delay for scheduled jobs",
    ["service", "class"],
    buckets=(0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5),
)

DEADLOCKS = Counter(
    "deadlocks_total",
    "Deadlocks detected and resolved",
    ["service", "strategy"],
)
