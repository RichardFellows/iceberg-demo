# Apache Iceberg Demo - Playwright Tests

Comprehensive end-to-end tests for both versions of the Apache Iceberg demo dashboards using Playwright.

## 📋 Test Coverage

### Full Version Dashboard (with MinIO/Nessie)
- System status verification (SQL Server, Nessie, MinIO)
- ETL pipeline execution and monitoring
- Backload operations
- Nessie catalog integration
- Progress tracking and logging
- Error handling
- Performance benchmarks
- Data integrity validation

### Filesystem Version Dashboard (without MinIO/Nessie)
- Filesystem mode indicators
- Reduced system components
- SQLite catalog operations
- Local storage ETL/backload
- Warning messages and limitations
- Performance with local storage
- Data consistency checks

## 🚀 Quick Start

### Prerequisites
- Node.js 20 or later
- Docker and Docker Compose
- 8GB RAM minimum
- Chromium browser (auto-installed)

### Installation

```bash
# Navigate to tests directory
cd tests

# Install dependencies
npm install

# Install Playwright browsers
npx playwright install --with-deps chromium
```

## 🧪 Running Tests

### Run All Tests
```bash
npm test
```

### Run Full Version Tests Only
```bash
npm run test:full
# or
npx playwright test tests/dashboard-full.spec.ts
```

### Run Filesystem Version Tests Only
```bash
npm run test:filesystem
# or
TEST_MODE=filesystem npx playwright test tests/dashboard-filesystem.spec.ts
```

### Interactive Mode
```bash
# Open Playwright UI
npm run test:ui

# Debug mode with browser
npm run test:debug

# Run with visible browser
npm run test:headed
```

### Specific Test Suites
```bash
# Run system status tests only
npx playwright test -g "System Status"

# Run ETL tests only
npx playwright test -g "ETL Pipeline"

# Run backload tests only
npx playwright test -g "Backload Pipeline"
```

## 📊 Test Reports

### View HTML Report
```bash
npm run test:report
# or
npx playwright show-report
```

### CI Reports
In CI environments, tests generate:
- GitHub Actions annotations
- HTML reports (uploaded as artifacts)
- Test videos on failure
- Screenshots on failure

## 🔧 Configuration

### Environment Variables

```bash
# Select test mode
TEST_MODE=full          # Test full version (default)
TEST_MODE=filesystem    # Test filesystem version

# Control teardown
STOP_AFTER_TEST=true   # Stop Docker services after tests
CI=true                # CI mode (affects retries and reporting)

# Custom endpoints
BASE_URL=http://localhost:8000  # Dashboard URL
```

### Timeouts

Default timeouts (configured in `playwright.config.ts`):
- Test timeout: 120 seconds
- Assertion timeout: 30 seconds
- Action timeout: 30 seconds
- ETL operations: 240 seconds
- Full cycle tests: 480 seconds

## 📁 Project Structure

```
tests/
├── package.json              # Node.js dependencies
├── playwright.config.ts      # Playwright configuration
├── README.md                # This file
├── tests/
│   ├── dashboard-full.spec.ts      # Full version tests
│   ├── dashboard-filesystem.spec.ts # Filesystem version tests
│   ├── global-setup.ts             # Start Docker services
│   ├── global-teardown.ts          # Stop Docker services
│   └── utils/
│       └── test-helpers.ts         # Shared test utilities
└── screenshots/             # Test screenshots (gitignored)
```

## 🧪 Test Scenarios

### Critical Path Tests
1. **System Health Check**
   - All services start correctly
   - Health endpoints respond
   - Database connectivity

2. **ETL Pipeline**
   - Execute ETL operation
   - Monitor progress
   - Verify table creation
   - Check row counts

3. **Backload Operation**
   - Requires ETL completion
   - Restore data to SQL Server
   - Validate data integrity

4. **End-to-End Flow**
   - Complete ETL → Backload cycle
   - Data consistency verification
   - Performance benchmarks

### Edge Cases
- Service failures
- Network timeouts
- Concurrent operations
- Permission errors
- Missing dependencies

## 🐛 Debugging

### View Browser During Tests
```bash
# Run with headed browser
npm run test:headed

# Debug specific test
npx playwright test --debug -g "should run ETL pipeline"
```

### Inspect Test Artifacts
```bash
# Screenshots on failure
ls test-results/*/screenshot.png

# Videos of failed tests
ls test-results/*/video.webm

# Trace files for debugging
npx playwright show-trace test-results/*/trace.zip
```

### Docker Logs
```bash
# View all service logs
docker-compose logs -f

# Specific service
docker-compose logs mssql
docker-compose logs dashboard
```

## 🚢 CI/CD Integration

### GitHub Actions
Tests run automatically on:
- Push to main/develop branches
- Pull requests
- Manual workflow dispatch

```yaml
# Manual trigger with options
workflow_dispatch:
  inputs:
    test_mode:
      - both
      - full
      - filesystem
```

### Local CI Simulation
```bash
# Run tests as in CI
CI=true npm test

# Generate GitHub annotations
npm run test:ci
```

## 📈 Performance Benchmarks

Expected timings (approximate):
- Dashboard load: < 5 seconds
- API status check: < 1 second
- ETL pipeline: 60-120 seconds
- Backload operation: 60-120 seconds
- Full cycle: 3-5 minutes

## 🔍 Test Helpers

The `test-helpers.ts` file provides utilities:

```typescript
// Wait for system health
await helpers.waitForSystemHealth(['SQL Server', 'MinIO']);

// Run ETL and wait for completion
await helpers.runETLPipeline();

// Check service status
const isHealthy = await helpers.isServiceHealthy('SQL Server');

// Get table information
const tables = await helpers.getSQLTables();
const icebergTables = await helpers.getIcebergTables();

// Verify row counts
await helpers.verifyTableCounts({
  'customers': 1000,
  'products': 500
});
```

## 🚨 Common Issues

### Services Not Starting
```bash
# Check Docker status
docker-compose ps

# Rebuild services
docker-compose down -v
docker-compose up -d --build
```

### Test Timeouts
```bash
# Increase timeout for slow systems
npx playwright test --timeout=300000
```

### Port Conflicts
```bash
# Check port usage
lsof -i :8000
lsof -i :1433

# Use different ports
BASE_URL=http://localhost:8001 npm test
```

## 📝 Writing New Tests

### Test Template
```typescript
test.describe('New Feature', () => {
  let helpers: DashboardHelpers;
  
  test.beforeEach(async ({ page }) => {
    helpers = new DashboardHelpers(page);
    await page.goto('/');
  });
  
  test('should do something', async ({ page }) => {
    // Your test here
    await helpers.waitForSystemHealth();
    await expect(page.locator('.element')).toBeVisible();
  });
});
```

### Best Practices
1. Use helpers for common operations
2. Set appropriate timeouts for long operations
3. Clean up after tests
4. Use descriptive test names
5. Group related tests in describe blocks
6. Handle both success and failure cases

## 📊 Test Metrics

Track test health with:
- Pass rate: Target > 95%
- Flakiness: Monitor retries in CI
- Duration: Track performance trends
- Coverage: Ensure critical paths tested

## 🤝 Contributing

1. Write tests for new features
2. Ensure tests pass locally
3. Update this README if needed
4. Submit PR with test results

## 📜 License

MIT License - Same as main project