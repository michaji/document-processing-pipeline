# Human Review Dashboard UI

This is a frontend dashboard built for human reviewers to validate, correct, and approve extracted document fields.

## Tech Stack
- **HTML5 & Vanilla JavaScript**: For lightweight and fast performance without heavy bundle sizes.
- **Vanilla CSS**: Custom styling with dynamic CSS variables to maintain a premium dark-mode aesthetic.
- **Vite**: Used for local fast development and building.
- **Lucide Icons**: Scalable icons across the dashboard.

## Features
- **Queue View**: Shows pending documents with Priority and SLA indicators.
- **Document Preview**: Visual display of the processed document.
- **Field Editing**: Low confidence fields highlighted, ability to modify values dynamically.
- **Keyboard Shortcuts**: `Ctrl + Enter` (or `Cmd + Enter`) to Confirm changes, `R` to Reject.
- **Real-time Stats**: Track reviewed items, queue depth, and SLA compliance.

## Development

To run locally:
```bash
npm install
npm run dev
```

The server will start on `http://localhost:5173`.
