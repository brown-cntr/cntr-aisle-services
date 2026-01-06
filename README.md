# CNTR Services

Backend microservices for the CNTR AISLE platform.

## Overview

CNTR Services handles automated processing of AI-related legislation through a pipeline of containerized services. These services run independently of the main Next.js application and communicate through a shared Supabase database.

## Architecture
[Link to Lucid flowchart](https://lucid.app/lucidchart/04290947-bb43-4203-a842-3cb754f1c47a/edit?viewport_loc=-328%2C-39%2C2478%2C1258%2CQYWkegBX7xCO&invitationId=inv_f5c444e7-f411-4334-bc6d-85822dc4ef90)

## Services

### Ingestion Service
**Purpose**: Fetch new AI-related bills from LegiScan and orchestrate downstream processing

**Responsibilities**:
- Daily polling of LegiScan API
- Filtering bills by AI-related keywords
- Parsing bill metadata (title, state, date introduced, etc.)
- Storing bill records in Supabase
- Enqueuing jobs for LLM and similarity processing

**Trigger**: Scheduled daily cron job

---

### LLM Service
**Purpose**: Generate AI-powered summaries and multi-perspective insights for each bill

**Responsibilities**:
- Generate concise bill summaries using GPT-4/Claude
- Generate insights from multiple perspectives:
  - Legislators (policy implications)
  - AI companies (business impact)
  - News reporters (public interest angles)
  - Other stakeholders
- Store results in Supabase bills table
- Handle LLM API rate limiting and retries

**Trigger**: On-demand via job queue when new bills are ingested

---

### Similarity Service
**Purpose**: Calculate semantic similarity between bills

**Responsibilities**:
- Generate embeddings for new bill text
- Compare against all existing bills in database
- Calculate similarity scores using 
- Update similarity matrix/relationships in Supabase
- Maintain in-memory embeddings cache for efficiency

**Trigger**: On-demand via job queue when new bills are ingested

---

## Technology Stack

- **Language**: Python
- **Queue**: RQ (Redis Queue) via Upstash Redis
- **Database**: Supabase (PostgreSQL)
- **Containerization**: Docker
- **Hosting**: Railway
- **CI/CD**: GitHub Actions
- **External APIs**: 
  - LegiScan API (bill data)
  - OpenAI/Anthropic APIs (LLM processing)

## Communication

Services are **loosely coupled** and communicate asynchronously:

- **Database**: Shared Supabase PostgreSQL database
- **Queue**: Redis-based job queue for task distribution
- **No direct HTTP**: Services don't call each other directly
```
Ingestion Service
    ↓ (writes to Supabase)
    ↓ (enqueues jobs to Redis)
    ↓
Queue (Redis)
    ↓ (workers pull jobs)
    ↓
LLM Service + Similarity Service
    ↓ (write results to Supabase)
    ↓
Frontend (cntr-web)
    ↓ (reads from Supabase)
```

## Local Development
```bash
# Clone repository
git clone https://github.com/yourusername/cntr-services.git
cd cntr-services

# Start all services with Docker Compose
docker-compose up

# Run specific service
docker-compose up ingestion
docker-compose up llm
docker-compose up similarity
```

## Deployment

Each service deploys independently to Railway:
```bash
# Deploy ingestion service
railway up --service ingestion

# Deploy LLM service
railway up --service llm

# Deploy similarity service
railway up --service similarity
```

Auto-deployment configured via GitHub Actions on push to `main`.

## Environment Variables

Required for all services:
- `SUPABASE_URL` - Supabase project URL
- `SUPABASE_KEY` - Supabase service role key
- `REDIS_URL` - Upstash Redis connection string

Service-specific:
- `LEGISCAN_API_KEY` - LegiScan API key (ingestion only)
- `OPENAI_API_KEY` - OpenAI API key (LLM only)
- `ANTHROPIC_API_KEY` - Anthropic API key (LLM only)

## Related Repositories

- **[cntr-web](https://github.com/cntr-aisle/cntr-webapp)**: Next.js frontend application (deployed to Vercel)
