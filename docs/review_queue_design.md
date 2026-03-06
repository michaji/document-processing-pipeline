# Review Queue System Design

## Data Model
`ReviewItem`:
- `id`: UUID
- `document_id`: Reference to original document
- `extracted_data`: JSON containing AI extracted fields and confidence scores.
- `status`: [PENDING, CLAIMED, COMPLETED, REJECTED]
- `assigned_to`: User ID of the reviewer (if CLAIMED)
- `priority`: Integer (1-100), calculated dynamically based on document type and SLA.
- `sla_deadline`: Timestamp for when the review must be completed.
- `created_at`: Timestamp
- `updated_at`: Timestamp

## Queue Operations
- **Priority Calculation**: Base priority is set by document type (e.g., invoices from VIP vendors get +20). As `sla_deadline` approaches, priority dynamically increases.
- **SLA-Aware Ordering**: The GET endpoint for next items returns records sorted by `sla_deadline` ascending, then `priority` descending.
- **Load-Balanced Assignment**: `CLAIM` operation atomically assigns a `ReviewItem` to a user. Uses optimistic locking (e.g., `WHERE status = PENDING AND version = X`) to ensure exactly-once assignment.

## API Design
- `GET /api/queue`: List pending items with filter/sort/pagination.
- `POST /api/queue/{id}/claim`: Atomically assign item to `user_id`.
- `POST /api/queue/{id}/submit`: Submit review. Payload includes corrected fields, outcome (`APPROVE`, `CORRECT`, `REJECT`), and reason.
- `GET /api/stats`: Return queue depth, average review time, items reviewed today.

## Feedback Loop
Corrections submitted via `POST /api/queue/{id}/submit` are saved as `FeedbackEvent` entries. A batch process periodically exports these events to retrain the ML model. The automated pipeline locks corrected fields by setting `_source = "HUMAN"` to prevent future AI overwrites.
