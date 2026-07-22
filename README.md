# UAILM — Credit Memo Generation System

## Architecture

```
Angular (bundled into Spring Boot jar)
    │
    │  POST /api/credit-memo/generate       Step 1+2
    │  GET  /api/credit-memo/events/{jobId} SSE (Step 5 + Step 9)
    │  GET  /api/credit-memo/status/{jobId} Polling fallback
    ▼
Spring Boot (uailm)
    │               │                    │
    ▼               ▼                    ▼
Azure SQL      Azure Managed Redis   Azure Managed Redis
(ASQL)         Streams               Pub/Sub + Hash
Persistent     ├─ outbound-stream    Fan-out SSE across
job state      │  uailm → ucldd      ASP instances +
               └─ inbound-stream     Status cache
                  ucldd → uailm
                       ↕
                     ucldd
                       ↕
                      LLM (~20 min)
```

---

## 9-Step Use Case

```
Step 1  User clicks "Start New Memo" in Angular
Step 2  uailm POST /generate → create job (PENDING) → XADD outbound stream → return jobId
Step 3  ucldd XREADGROUP outbound stream → sends ACK to inbound stream
Step 4  uailm InboundStreamListener → reads ACK → updateDB(IN_PROGRESS)
Step 5  uailm → Redis Pub/Sub → all instances → SSE push → Angular flips to "In Process"
Step 6  LLM processes (~20 minutes)
Step 7  ucldd publishes RESPONSE to inbound stream
Step 8  uailm InboundStreamListener → reads RESPONSE → updateDB(COMPLETED) + store raw content
Step 9  uailm → Redis Pub/Sub → all instances → SSE push → Angular flips to "Completed" + shows memo
```

---

## Multi-Instance SSE Solution

```
3 ASP Instances running:

Instance 1              Instance 2              Instance 3
Angular SSE → here      Stream listener fires   (idle)
emitter stored locally
                        markCompleted()
                        PUBLISH memo-notifications (Redis Pub/Sub)
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
         Instance 1      Instance 2      Instance 3
         onMessage()     onMessage()     onMessage()
         emitter found   emitter=null    emitter=null
         → SSE push! ✓   → skip          → skip
```

Consumer group ensures only ONE instance processes each stream message.
Redis Pub/Sub ensures the instance holding the SSE emitter pushes to Angular.
HOSTNAME env var (set by Azure ASP automatically) gives each instance a unique consumer name.

---

## Redis Keys

| Key / Channel                    | Type    | Purpose                          | TTL   |
|----------------------------------|---------|----------------------------------|-------|
| `credit-memo-request-stream`     | Stream  | uailm → ucldd outbound           | none  |
| `credit-memo-response-stream`    | Stream  | ucldd → uailm inbound            | none  |
| `uailm-memo-notifications`       | Pub/Sub | SSE fan-out across instances     | -     |
| `uailm:status:{jobId}`           | Hash    | Fast status cache for polling    | 4 hrs |

---

## Inbound Stream Message Contract (ucldd → uailm)

**ACK** (arrives seconds after request):
```
jobId          = "abc123"
type           = "ACK"
```

**RESPONSE** (~20 minutes later):
```
jobId          = "abc123"
type           = "RESPONSE"
success        = "true" | "false"
resultContent  = "<raw memo content>"
errorMessage   = "<error if success=false>"
```

---

## Project Structure

```
uailm/
├── docker-compose.yml
├── pom.xml
├── src/main/
│   ├── resources/
│   │   ├── application.yml           ← prod (env vars)
│   │   └── application-local.yml     ← local dev
│   └── java/com/uailm/
│       ├── UailmApplication.java
│       ├── config/
│       │   └── RedisConfig.java      ← template + stream container + pub/sub container
│       ├── controller/
│       │   └── CreditMemoController.java  ← /generate, /events/{jobId}, /status/{jobId}
│       ├── service/
│       │   ├── OutboundStreamService.java     ← XADD request stream
│       │   ├── JobStatusService.java          ← ASQL + Redis state
│       │   ├── JobStatusResponse.java         ← poll DTO + uiMessage mapping
│       │   ├── SseEmitterRegistry.java        ← holds open SSE connections (per instance)
│       │   └── MemoNotificationPublisher.java ← Redis Pub/Sub publisher
│       ├── listener/
│       │   ├── InboundStreamListener.java     ← XREADGROUP response stream
│       │   └── MemoNotificationSubscriber.java ← Pub/Sub subscriber (all instances)
│       └── model/
│           ├── MemoRequest.java
│           ├── MemoRequestRepository.java
│           ├── JobStatus.java
│           ├── OutboundMessage.java
│           └── InboundMessage.java
└── frontend/src/app/
    ├── services/
    │   └── credit-memo.service.ts    ← generate() + connectAndListen() (SSE + poll fallback)
    └── components/credit-memo/
        ├── credit-memo.component.ts  ← 6-state machine matching 9 use case steps
        ├── credit-memo.component.html
        └── credit-memo.component.scss
```

---

## Environment Variables

```bash
AZURE_SQL_HOST      = your-server.database.windows.net
AZURE_SQL_DB        = uailm_db
AZURE_SQL_USERNAME  = your-user
AZURE_SQL_PASSWORD  = your-password
REDIS_HOST          = your-cache.redis.cache.windows.net
REDIS_PASSWORD      = your-access-key
CORS_ALLOWED_ORIGINS = https://your-app.azurewebsites.net
# HOSTNAME is set automatically by Azure ASP per instance — no action needed
```

---

## Local Development

```bash
# Start Redis + SQL Server
docker-compose up -d

# Run Spring Boot with local profile
mvn spring-boot:run -Dspring-boot.run.profiles=local

# Run Angular
cd frontend && npm install && ng serve
```
