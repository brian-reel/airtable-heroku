# Airtable-Heroku Sync System

A robust synchronization system between PostgreSQL and Airtable, designed to run on Heroku.

## Project Overview

This application synchronizes data between a PostgreSQL database and Airtable, focusing on:

- Employee data synchronization
- Guard card information
- Employee roles
- Training data
- Duplicate detection and management
- Contact information

## Directory Structure

```
/
├── src/                  # Source code
│   ├── config/           # Configuration files
│   ├── services/         # Database and API services
│   ├── sync/             # Synchronization modules
│   ├── utils/            # Utility functions
│   ├── reports/          # Report output directory
│   │   └── history/      # Historical reports
│   ├── middleware/       # Any middleware (if needed)
│   └── models/           # Data models (if needed)
├── scripts/              # Utility scripts
└── tests/                # Test files
    ├── unit/             # Unit tests
    └── integration/      # Integration tests
```

## Key Components

### Configuration

- `src/config/config.js` - Central configuration
- `src/config/database.js` - Database connection setup

### Services

- `src/services/airtable.js` - Airtable API interactions
- `src/services/postgres.js` - PostgreSQL database operations
- `src/services/lms.js` - LMS API interactions

### Sync Modules

- `src/sync/employee-sync.js` - Employee data synchronization
- `src/sync/guard-cards.js` - Guard card information sync
- `src/sync/roles.js` - Employee roles sync
- `src/sync/training.js` - Training data sync
- `src/sync/duplicates.js` - Duplicate detection
- `src/sync/contact-info.js` - Contact information sync

### Utilities

- `src/utils/logging.js` - Logging utilities
- `src/utils/validation.js` - Data validation
- `src/utils/error-handler.js` - Error handling
- `src/utils/formatters.js` - Data formatting utilities

## Setup Instructions

### Prerequisites

- Node.js v16.17.0
- npm 8.15.0
- PostgreSQL database
- Airtable account with API access

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/brian-reel/airtable-heroku.git
   cd airtable-heroku
   ```

2. Create the directory structure:
   ```
   node scripts/create-structure.js
   ```

3. Set up environment variables:
   ```
   cp .env.example .env
   ```
   Then edit the `.env` file with your actual credentials.

4. Install dependencies:
   ```
   npm install
   ```

5. Run the application:
   ```
   npm start
   ```

### Project Organization

If you need to organize the project structure or standardize code patterns, use the provided scripts:

1. Organize project structure:
   ```
   node scripts/organize-project.js
   ```

2. Standardize code patterns:
   ```
   node scripts/standardize-code.js
   ```

## Deployment

### Deploying to Heroku

1. Create a Heroku app:
   ```
   heroku create your-app-name
   ```

2. Add Heroku as a remote:
   ```
   heroku git:remote -a your-app-name
   ```

3. Set environment variables:
   ```
   heroku config:set PG_CONNECTION_STRING=your_connection_string
   heroku config:set AIRTABLE_API_KEY=your_api_key
   heroku config:set AIRTABLE_BASE_ID=your_base_id
   # Set other environment variables as needed
   ```

4. Deploy to Heroku:
   ```
   git push heroku main
   ```

## Development Guidelines

### Code Style

- Use consistent naming conventions (camelCase for variables and functions)
- Add JSDoc comments to all functions
- Use the centralized error handling with `tryCatch` wrapper
- Use structured logging with appropriate levels

### Adding New Features

1. Create appropriate service modules in `src/services/`
2. Implement sync logic in `src/sync/`
3. Update configuration if needed
4. Add tests in `tests/`
5. Update documentation

### Error Handling

All asynchronous functions should be wrapped with the `tryCatch` utility:

```javascript
async function myFunction() {
  return tryCatch(async () => {
    // Function implementation
  }, 'my_function_error_code', { additionalContext });
}
```

### Logging

Use the structured logging utilities:

```javascript
// Start a sync process
logSyncStart('process_name');

// Log to history
await logToHistory('Important event occurred');

// End a sync process with stats
logSyncEnd('process_name', { 
  'Records processed': 100,
  'Updates made': 50
});
```

## Troubleshooting

### Common Issues

1. **Connection errors**: Verify your database connection strings and API keys in the `.env` file.

2. **Rate limiting**: Airtable has rate limits. The application includes delay mechanisms, but you may need to adjust them in `config.js`.

3. **Duplicate function declarations**: If you encounter errors about duplicate function declarations, run the standardization script:
   ```
   node scripts/standardize-code.js
   ```

### Logs and Reports

- Check `src/reports/` for detailed sync reports
- Historical logs are stored in `src/reports/history/`

## License

[MIT License](LICENSE)

## Contact

For questions or support, please contact [your-email@example.com](mailto:your-email@example.com)
