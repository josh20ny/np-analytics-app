# NP Analytics App

This document explains the structure of the NP Analytics App and what each file and folder does.

```
project_root/
├── $
├── .DS_Store
├── .gitignore
├── README.md
├── app
    ├── .DS_Store
    ├── __init__.py
    ├── attendance.py
    ├── config.py
    ├── db.py
    ├── google_sheets.py
    ├── mailchimp.py
    ├── models.py
    ├── planning_center
    │   ├── __init__.py
    │   ├── checkins.py
    │   └── groups.py
    └── youtube
    │   ├── __init__.py
    │   ├── analytics_api.py
    │   ├── data_api.py
    │   └── routes.py
├── clickup_app
    ├── __init__.py
    ├── clickup_client.py
    ├── config.py
    ├── crud.py
    ├── database.py
    ├── models.py
    ├── oauth_routes.py
    └── webhooks.py
├── dashboard
    ├── config.py
    ├── data.py
    ├── main.py
    └── widgets.py
├── main.py
├── requirements.txt
├── run_jobs.py
├── token.json
├── weekly_summary
    ├── __init__.py
    ├── config.py
    ├── data_access.py
    ├── formatter.py
    └── main.py
└── youtube_auth.py


## Overview

- **main.py**
  Main controller for the npanalyticsapp. 

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

  - **dashboard/**  
  Creates a visual dashboard of the data storred in PostgreSQL
  - `config.py`: Dashboard page and tabs setup configuration
  - `data.py`: pulls in data from PostgreSQL
  - `main.py`: launches the dashboard
  - `widgets.py`: collection of different data-view functions

  - **weekly_summary/**  
  Sends a weekly Summary of the data to ClickUP via API call
  - `config.py`: configures env variables
  - `data_access.py`: pulls in data from PostgreSQL
  - `main.py`: launches the dashboard
  - `formatter.py`: formats the data to a nice multiline text block to be sent in ClickUp Chat

  - **clickup_app/**  
  Sets up the clickup interface
  - `clickup_client.py`: handles clickup interfacing
  - `config.py`: configures env variables
  - `crud.py`: handles retreiving / updating the access token
  - `database.py`: pulls in data from PostgreSQL
  - `models.py`: token scaffolding
  - `oauth_routes.py`: handles oauth calls
  - `webhooks.py`: handles sending and receiving
  
- **run_jobs.py**  
  Hits all of the API routes to actually pull the data and store it in the 
  database. This is setup to run every monday morning at 8 am CST. 

- **youtube_auth.py**  
  Contains the OAuth2 flow and credential setup for interacting with  
  the YouTube Analytics API.

- **.env**  
  Stores all sensitive configuration—API keys, DB credentials, and IDs.

- **requirements.txt**  
  Lists all Python packages your project depends on.

NP Analytics App is stored and run on render.com.  
