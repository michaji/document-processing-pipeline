# Document Processing Pipeline

## Project Overview
This project is an AI-powered document processing platform designed for scale.

## Architecture
- **Part A:** Module Interface & Idempotency logic implemented in `src/extraction_module.py`.
- **Part B:** Workflow Engine (DAG-based) implemented in `src/workflow_executor.py`.
- **Part C:** Human Review UI built with HTML/CSS/JS (Vite environment) inside `ui/`.
- **Part D:** SLA Monitoring implemented in `src/monitoring.py` and `configs/`.

## Setup
```bash
pip install -r requirements.txt
pytest tests/
```

## Running the UI
```bash
cd ui
npm install
npm run dev
```
