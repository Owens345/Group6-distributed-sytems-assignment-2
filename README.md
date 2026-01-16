# Carrier Grade Edge.Core.Cloud Demo System

This repo is a working multi service prototype that demonstrates:
- Edge.Core.Cloud placement and flows
- RPC between services
- Event ordering and async processing
- Distributed shared memory (DSM) built on Redis with versioning
- Distributed transactions with 2PC and 3PC
- Fault tolerance patterns: leader election, retries, timeouts
- Byzantine style faults: invalid signatures are rejected
- Quantitative evaluation: Prometheus metrics endpoints for every service
- A simple frontend to drive demos live

## Services
- `edge_gateway` (Edge). Frontend + API. Routes traffic. Local cache. Demo controls.
- `session_manager` (Core). Transaction coordinator. Leader election. Session state.
- `resource_manager` (Core). Participant for resource reservation.
- `billing_service` (Core/Cloud). Participant for billing and CDR trigger.
- `analytics_service` (Cloud). Consumes ordered events and computes aggregates.
- `malicious_node` (Test tool). Sends bad signatures to show Byzantine handling.

## Quick start (local, easiest)
You need Python 3.10+ and Redis.

1. Start Redis on localhost:6379
2. Create venv and install deps:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. Start services in separate terminals:
   ```bash
   uvicorn edge_gateway.app:app --port 8000
   uvicorn session_manager.app:app --port 8001
   uvicorn resource_manager.app:app --port 8002
   uvicorn billing_service.app:app --port 8003
   uvicorn analytics_service.app:app --port 8004
   ```
4. Open the frontend:
   - http://localhost:8000

## Demo flow
- Create sessions using the UI
- Toggle 2PC vs 3PC
- Inject faults: crash simulation, slow participant, random aborts, network delay simulation
- Run a mini load test from the UI
- View live metrics and logs in the UI

## Metrics
Each service exposes Prometheus metrics at:
- `/metrics`

The frontend also summarizes key metrics by polling service status endpoints.

## Notes
- For simplicity this prototype uses Redis for DSM and for ordered event streams (Redis Streams).
  During containerization you can swap to Kafka and etcd if you want a more carrier grade stack.
- Persistence uses SQLite files per service so it runs anywhere without extra setup.


## Docker Compose (recommended for presentation)
```bash
docker compose up -d --build
```

Open:
- Frontend: http://localhost:8000
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)

## New demos
- Deadlock demo: use the UI button. It triggers a deterministic deadlock and resolves by timeout abort.
- Scheduling demo: choose VOICE vs DATA when creating sessions, then run load test and observe queue delay in Grafana.
