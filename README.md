# NP Analytics App

This document explains the structure of the NP Analytics App and what each file and folder does.

```
project_root/
│
├─ app/
│   ├─ __init__.py         # Marks 'app' as a Python package
│   ├─ main.py             # Entry point: initializes FastAPI, mounts routers
│   ├─ config.py           # Loads environment variables via python-dotenv
│   ├─ db.py               # Database connection helper (psycopg2 wrapper)
│   ├─ models.py           # Shared Pydantic models (e.g., AttendanceInput)
│   ├─ google_sheets.py    # Google Sheets integration and /google-sheets endpoints
│   ├─ attendance.py       # /attendance endpoints and sheet-processing logic
│   ├─ mailchimp.py        # /mailchimp endpoint logic and database saving
│   │
│   ├─ planning_center/    # Planning Center Online integrations
│   │   ├─ __init__.py     # Marks planning_center/ as a package
│   │   ├─ checkins.py     # fetch/process check-ins, summarize by ministry, insert into DB
│   │   └─ groups.py       # fetch/process groups summary, insert into DB
│   │
│   └─ youtube/            # YouTube Data & Analytics integrations
│       ├─ __init__.py
│       ├─ data_api.py     # YouTube Data API calls (search & video stats)
│       ├─ analytics_api.py# YouTube Analytics API calls (weekly summary, watch time)
│       └─ routes.py       # /youtube endpoints (livestream tracking & weekly-summary)
│
├─ scheduler.py            # APScheduler script: triggers endpoints every Monday at 8 AM CT
├─ youtube_auth.py         # OAuth2 authentication wrapper for YouTube Analytics API
├─ .env                    # Environment variables and secrets
├─ requirements.txt        # Python dependencies
└─ README.md               # This file

## Overview

- **app/**  
  Houses all FastAPI routers and utility modules, organized by concern:
  - `config.py`: centralizes loading of env vars
  - `db.py`: connection factory for PostgreSQL
  - `models.py`: shared schemas
  - `google_sheets.py`: Google Sheets read/write & test route
  - `attendance.py`: both manual and scheduled adult attendance processing
  - `mailchimp.py`: weekly Mailchimp metrics retrieval & storage
  - `planning_center/`: subpackage for PCO check-ins and groups summaries
  - `youtube/`: subpackage for YouTube Data vs Analytics API and related routes

- **scheduler.py**  
  Uses APScheduler to call key endpoints (`/attendance/process-sheet`,  
  `/mailchimp/weekly-summary`, `/youtube/weekly-summary`) every Monday at 8 AM CT.

- **youtube_auth.py**  
  Contains the OAuth2 flow and credential setup for interacting with  
  the YouTube Analytics API.

- **.env**  
  Stores all sensitive configuration—API keys, DB credentials, and IDs.

- **requirements.txt**  
  Lists all Python packages your project depends on.

With this structure, each integration is isolated in its own module, making  
testing, maintenance, and future expansions straightforward.  
