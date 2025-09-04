import { test, expect } from '@playwright/test';
import { DashboardHelpers, retryOperation } from './utils/test-helpers';

/**
 * Full version dashboard tests (with MinIO and Nessie)
 */
test.describe('Apache Iceberg Demo - Full Version Dashboard', () => {
  let helpers: DashboardHelpers;

  test.beforeAll(async () => {
    // Ensure we're using the full docker-compose
    process.env.COMPOSE_FILE = 'docker-compose.yml';
  });

  test.beforeEach(async ({ page }) => {
    helpers = new DashboardHelpers(page);
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test.describe('System Status', () => {
    test('should display all system components', async ({ page }) => {
      // Check for all expected services
      await expect(page.locator('.status').filter({ hasText: 'SQL Server' })).toBeVisible();
      await expect(page.locator('.status').filter({ hasText: 'Nessie' })).toBeVisible();
      await expect(page.locator('.status').filter({ hasText: 'MinIO' })).toBeVisible();
    });

    test('should show healthy status for all services', async ({ page }) => {
      // Wait for services to be healthy
      await helpers.waitForSystemHealth(['SQL Server', 'Nessie', 'MinIO']);
      
      // Verify each service is healthy
      expect(await helpers.isServiceHealthy('SQL Server')).toBeTruthy();
      expect(await helpers.isServiceHealthy('Nessie')).toBeTruthy();
      expect(await helpers.isServiceHealthy('MinIO')).toBeTruthy();
    });

    test('should display correct API endpoints', async ({ page }) => {
      const apiStatus = await helpers.checkAPIStatus();
      
      expect(apiStatus.sql_server).toBeDefined();
      expect(apiStatus.nessie).toBeDefined();
      expect(apiStatus.minio).toBeDefined();
    });
  });

  test.describe('Dashboard UI Elements', () => {
    test('should display all dashboard sections', async ({ page }) => {
      // Header
      await expect(page.locator('h1').filter({ hasText: 'Apache Iceberg Demo Dashboard' })).toBeVisible();
      
      // Cards
      await expect(page.locator('.card').filter({ hasText: 'System Status' })).toBeVisible();
      await expect(page.locator('.card').filter({ hasText: 'ETL Controls' })).toBeVisible();
      await expect(page.locator('.card').filter({ hasText: 'SQL Server Tables' })).toBeVisible();
      await expect(page.locator('.card').filter({ hasText: 'Nessie Catalog' })).toBeVisible();
      await expect(page.locator('.card').filter({ hasText: 'Operation Log' })).toBeVisible();
    });

    test('should display ETL control buttons', async ({ page }) => {
      await expect(page.locator('#run-etl')).toBeVisible();
      await expect(page.locator('#run-etl')).toHaveText(/Run ETL Pipeline/);
      
      await expect(page.locator('#run-backload')).toBeVisible();
      await expect(page.locator('#run-backload')).toHaveText(/Run Backload/);
    });

    test('should display SQL Server tables with row counts', async ({ page }) => {
      await helpers.waitForSystemHealth(['SQL Server']);
      
      const tables = await helpers.getSQLTables();
      expect(tables.length).toBeGreaterThan(0);
      
      // Verify expected tables exist
      const tableNames = tables.map(t => t.name);
      expect(tableNames).toContain('customers');
      expect(tableNames).toContain('products');
      expect(tableNames).toContain('transactions');
      expect(tableNames).toContain('order_details');
      expect(tableNames).toContain('inventory_snapshots');
      
      // Verify row counts
      await helpers.verifyTableCounts({
        'customers': 1000,
        'products': 500,
        'transactions': 10000,
        'order_details': 5000,
        'inventory_snapshots': 2000
      });
    });
  });

  test.describe('ETL Pipeline', () => {
    test('should run ETL pipeline successfully', async ({ page }) => {
      test.setTimeout(240000); // 4 minutes for ETL
      
      await helpers.waitForSystemHealth(['SQL Server', 'Nessie', 'MinIO']);
      
      // Run ETL
      await helpers.runETLPipeline();
      
      // Verify completion
      const logs = await helpers.getOperationLogs();
      expect(logs).toContain('ETL completed successfully');
      expect(logs).toContain('18500 rows migrated');
      
      // Verify Iceberg tables created
      const icebergTables = await helpers.getIcebergTables();
      expect(icebergTables.length).toBe(5);
    });

    test('should show progress during ETL', async ({ page }) => {
      test.setTimeout(240000);
      
      await helpers.waitForSystemHealth(['SQL Server', 'Nessie', 'MinIO']);
      
      // Click ETL button
      const etlButton = page.locator('#run-etl');
      await etlButton.click();
      
      // Verify progress bar appears
      await expect(page.locator('#progress-container')).toBeVisible();
      await expect(page.locator('#progress-bar')).toBeVisible();
      
      // Check progress increases
      const initialProgress = await helpers.getProgress();
      await page.waitForTimeout(5000);
      const laterProgress = await helpers.getProgress();
      
      expect(laterProgress).toBeGreaterThanOrEqual(initialProgress);
    });

    test('should disable buttons during ETL execution', async ({ page }) => {
      await helpers.waitForSystemHealth(['SQL Server', 'Nessie', 'MinIO']);
      
      // Start ETL
      const etlButton = page.locator('#run-etl');
      await etlButton.click();
      
      // Both buttons should be disabled
      await expect(page.locator('#run-etl')).toBeDisabled();
      await expect(page.locator('#run-backload')).toBeDisabled();
    });

    test('should log ETL operations', async ({ page }) => {
      test.setTimeout(240000);
      
      await helpers.waitForSystemHealth(['SQL Server', 'Nessie', 'MinIO']);
      
      // Clear any existing logs
      await helpers.clearLogs();
      
      // Run ETL
      await helpers.runETLPipeline();
      
      // Check for expected log entries
      const logs = await helpers.getOperationLogs();
      expect(logs).toContain('Starting ETL pipeline');
      expect(logs).toContain('Connecting to SQL Server');
      expect(logs).toContain('Initializing Nessie REST catalog');
      expect(logs).toContain('Migrating customers');
      expect(logs).toContain('Migrating products');
      expect(logs).toContain('Migrating transactions');
      expect(logs).toContain('ETL completed successfully');
    });
  });

  test.describe('Backload Pipeline', () => {
    test('should run backload successfully after ETL', async ({ page }) => {
      test.setTimeout(360000); // 6 minutes for ETL + backload
      
      await helpers.waitForSystemHealth(['SQL Server', 'Nessie', 'MinIO']);
      
      // First run ETL
      await helpers.runETLPipeline();
      
      // Then run backload
      await helpers.runBackload();
      
      // Verify completion
      const logs = await helpers.getOperationLogs();
      expect(logs).toContain('Backload completed successfully');
      expect(logs).toContain('18500 rows backloaded');
    });

    test('should restore data to SQL Server', async ({ page }) => {
      test.setTimeout(360000);
      
      await helpers.waitForSystemHealth(['SQL Server', 'Nessie', 'MinIO']);
      
      // Run ETL first
      await helpers.runETLPipeline();
      
      // Run backload
      await helpers.runBackload();
      
      // Check API to verify backloaded tables
      const apiStatus = await helpers.checkAPIStatus();
      
      // Should have backload tables in target database
      expect(apiStatus.backload_tables).toBeDefined();
      if (apiStatus.backload_tables) {
        expect(apiStatus.backload_tables).toContain('customers_backload');
        expect(apiStatus.backload_tables).toContain('products_backload');
        expect(apiStatus.backload_tables).toContain('transactions_backload');
      }
    });
  });

  test.describe('Nessie Catalog Integration', () => {
    test('should display Nessie catalog information', async ({ page }) => {
      await helpers.waitForSystemHealth(['Nessie']);
      
      // Check for Nessie catalog section
      await expect(page.locator('.card').filter({ hasText: 'Nessie Catalog' })).toBeVisible();
    });

    test('should show Iceberg tables in Nessie after ETL', async ({ page }) => {
      test.setTimeout(240000);
      
      await helpers.waitForSystemHealth(['SQL Server', 'Nessie', 'MinIO']);
      
      // Run ETL
      await helpers.runETLPipeline();
      
      // Check Nessie tables
      const nessieTables = await helpers.getNessieTables();
      expect(nessieTables.length).toBeGreaterThan(0);
    });
  });

  test.describe('Error Handling', () => {
    test('should handle API errors gracefully', async ({ page }) => {
      // Try to access non-existent endpoint
      const response = await page.request.get('/api/nonexistent');
      expect(response.status()).toBe(404);
    });

    test('should show error message if ETL fails', async ({ page, context }) => {
      // Mock ETL failure by intercepting the API call
      await context.route('/api/etl/run', route => {
        route.fulfill({
          status: 500,
          contentType: 'application/json',
          body: JSON.stringify({ status: 'error', message: 'Test error' })
        });
      });
      
      const etlButton = page.locator('#run-etl');
      await etlButton.click();
      
      // Should show error in logs
      await page.waitForTimeout(2000);
      const logs = await helpers.getOperationLogs();
      expect(logs).toContain('Failed to start ETL');
    });
  });

  test.describe('Auto-refresh', () => {
    test('should auto-refresh status periodically', async ({ page }) => {
      await helpers.waitForSystemHealth(['SQL Server']);
      
      // Get initial API call count
      let apiCallCount = 0;
      await page.route('/api/status', route => {
        apiCallCount++;
        route.continue();
      });
      
      // Wait for auto-refresh (30 seconds + buffer)
      await page.waitForTimeout(35000);
      
      // Should have made multiple API calls
      expect(apiCallCount).toBeGreaterThan(1);
    });
  });

  test.describe('Responsive Design', () => {
    test('should be responsive on mobile', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      
      // Dashboard should still be functional
      await expect(page.locator('h1')).toBeVisible();
      await expect(page.locator('#run-etl')).toBeVisible();
      await expect(page.locator('#run-backload')).toBeVisible();
    });

    test('should be responsive on tablet', async ({ page }) => {
      // Set tablet viewport
      await page.setViewportSize({ width: 768, height: 1024 });
      
      // All cards should be visible
      const cards = await page.locator('.card').all();
      expect(cards.length).toBeGreaterThan(3);
    });
  });

  test.describe('Data Integrity', () => {
    test('should maintain data integrity through ETL and backload', async ({ page }) => {
      test.setTimeout(480000); // 8 minutes for full cycle
      
      await helpers.waitForSystemHealth(['SQL Server', 'Nessie', 'MinIO']);
      
      // Get initial row counts
      const initialTables = await helpers.getSQLTables();
      const initialCounts: Record<string, number> = {};
      initialTables.forEach(t => {
        initialCounts[t.name] = t.rowCount;
      });
      
      // Run ETL
      await helpers.runETLPipeline();
      
      // Verify Iceberg tables created
      const icebergTables = await helpers.getIcebergTables();
      expect(icebergTables.length).toBe(5);
      
      // Run backload
      await helpers.runBackload();
      
      // Verify data integrity
      const logs = await helpers.getOperationLogs();
      expect(logs).toContain('✓ Validation passed');
      
      // Total row count should match
      const totalInitialRows = Object.values(initialCounts).reduce((a, b) => a + b, 0);
      expect(logs).toContain(`${totalInitialRows} rows`);
    });
  });

  test.describe('Performance', () => {
    test('dashboard should load quickly', async ({ page }) => {
      const startTime = Date.now();
      await page.goto('/');
      await page.waitForLoadState('networkidle');
      const loadTime = Date.now() - startTime;
      
      // Should load in under 5 seconds
      expect(loadTime).toBeLessThan(5000);
    });

    test('API status endpoint should respond quickly', async ({ page }) => {
      const startTime = Date.now();
      await helpers.checkAPIStatus();
      const responseTime = Date.now() - startTime;
      
      // Should respond in under 1 second
      expect(responseTime).toBeLessThan(1000);
    });
  });
});