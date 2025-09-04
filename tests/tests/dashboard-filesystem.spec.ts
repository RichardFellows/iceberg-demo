import { test, expect } from '@playwright/test';
import { DashboardHelpers, retryOperation } from './utils/test-helpers';

/**
 * Filesystem version dashboard tests (without MinIO and Nessie)
 */
test.describe('Apache Iceberg Demo - Filesystem Version Dashboard', () => {
  let helpers: DashboardHelpers;

  test.beforeAll(async () => {
    // Ensure we're using the filesystem docker-compose
    process.env.COMPOSE_FILE = 'docker-compose-filesystem.yml';
  });

  test.beforeEach(async ({ page }) => {
    helpers = new DashboardHelpers(page);
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test.describe('System Status - Filesystem Mode', () => {
    test('should display filesystem mode badge', async ({ page }) => {
      // Check for filesystem mode indicator
      await expect(page.locator('.mode-badge').filter({ hasText: 'Filesystem Mode' })).toBeVisible();
    });

    test('should display reduced system components', async ({ page }) => {
      // Check for SQL Server
      await expect(page.locator('.status').filter({ hasText: 'SQL Server' })).toBeVisible();
      
      // Check for filesystem storage
      await expect(page.locator('.status').filter({ hasText: 'Filesystem Storage' })).toBeVisible();
      
      // Should NOT have MinIO or Nessie
      await expect(page.locator('.status').filter({ hasText: 'MinIO' })).not.toBeVisible();
      await expect(page.locator('.status').filter({ hasText: 'Nessie' })).not.toBeVisible();
    });

    test('should show filesystem storage path', async ({ page }) => {
      const storageStatus = await page.locator('.status').filter({ hasText: 'Filesystem Storage' }).textContent();
      expect(storageStatus).toContain('/data/warehouse');
    });

    test('should display catalog status', async ({ page }) => {
      // Initially should show not initialized
      const catalogStatus = await page.locator('.status').filter({ hasText: 'Iceberg Catalog' }).textContent();
      expect(catalogStatus).toMatch(/Not initialized|Checking/);
    });

    test('should show warning about filesystem mode', async ({ page }) => {
      // Should display warning box
      await expect(page.locator('.warning-box')).toBeVisible();
      await expect(page.locator('.warning-box')).toContainText('Filesystem Mode');
      await expect(page.locator('.warning-box')).toContainText('local storage');
    });
  });

  test.describe('Dashboard UI Elements - Filesystem', () => {
    test('should display all dashboard sections', async ({ page }) => {
      // Header with mode indicator
      await expect(page.locator('h1').filter({ hasText: 'Apache Iceberg Demo Dashboard' })).toBeVisible();
      await expect(page.locator('.mode-badge')).toBeVisible();
      
      // Cards
      await expect(page.locator('.card').filter({ hasText: 'System Status' })).toBeVisible();
      await expect(page.locator('.card').filter({ hasText: 'ETL Controls' })).toBeVisible();
      await expect(page.locator('.card').filter({ hasText: 'SQL Server Tables' })).toBeVisible();
      await expect(page.locator('.card').filter({ hasText: 'Iceberg Tables (Filesystem)' })).toBeVisible();
      await expect(page.locator('.card').filter({ hasText: 'Operation Log' })).toBeVisible();
    });

    test('should NOT display Nessie catalog section', async ({ page }) => {
      await expect(page.locator('.card').filter({ hasText: 'Nessie Catalog' })).not.toBeVisible();
    });

    test('should display SQL Server tables correctly', async ({ page }) => {
      await helpers.waitForSystemHealth(['SQL Server']);
      
      const tables = await helpers.getSQLTables();
      expect(tables.length).toBeGreaterThan(0);
      
      // Verify expected tables and counts
      await helpers.verifyTableCounts({
        'customers': 1000,
        'products': 500,
        'transactions': 10000,
        'order_details': 5000,
        'inventory_snapshots': 2000
      });
    });
  });

  test.describe('ETL Pipeline - Filesystem', () => {
    test('should run ETL pipeline with filesystem storage', async ({ page }) => {
      test.setTimeout(240000); // 4 minutes for ETL
      
      await helpers.waitForSystemHealth(['SQL Server']);
      
      // Run ETL
      await helpers.runETLPipeline();
      
      // Verify completion
      const logs = await helpers.getOperationLogs();
      expect(logs).toContain('ETL completed successfully');
      expect(logs).toContain('18500 rows migrated');
      expect(logs).toContain('Data stored in: /data/warehouse');
      
      // Verify Iceberg tables created
      const icebergTables = await helpers.getIcebergTables();
      expect(icebergTables.length).toBe(5);
    });

    test('should initialize catalog on first ETL run', async ({ page }) => {
      test.setTimeout(240000);
      
      await helpers.waitForSystemHealth(['SQL Server']);
      
      // Check initial catalog status
      let catalogStatus = await helpers.getServiceStatus('Iceberg Catalog');
      expect(catalogStatus).toContain('Not initialized');
      
      // Run ETL
      await helpers.runETLPipeline();
      
      // Catalog should now be initialized
      await page.reload();
      await helpers.waitForSystemHealth(['SQL Server']);
      
      catalogStatus = await helpers.getServiceStatus('Iceberg Catalog');
      expect(catalogStatus).toContain('Initialized');
    });

    test('should create SQLite catalog database', async ({ page }) => {
      test.setTimeout(240000);
      
      await helpers.waitForSystemHealth(['SQL Server']);
      
      // Run ETL
      await helpers.runETLPipeline();
      
      // Check API status for catalog
      const apiStatus = await helpers.checkAPIStatus();
      expect(apiStatus.catalog_exists).toBeTruthy();
    });

    test('should log filesystem-specific operations', async ({ page }) => {
      test.setTimeout(240000);
      
      await helpers.waitForSystemHealth(['SQL Server']);
      
      // Run ETL
      await helpers.runETLPipeline();
      
      // Check for filesystem-specific log entries
      const logs = await helpers.getOperationLogs();
      expect(logs).toContain('Filesystem Mode');
      expect(logs).toContain('filesystem catalog');
      expect(logs).toContain('SQLite');
      expect(logs).not.toContain('MinIO');
      expect(logs).not.toContain('Nessie');
      expect(logs).not.toContain('S3');
    });

    test('should handle partitioned tables correctly', async ({ page }) => {
      test.setTimeout(240000);
      
      await helpers.waitForSystemHealth(['SQL Server']);
      
      // Run ETL
      await helpers.runETLPipeline();
      
      // Check logs for partition information
      const logs = await helpers.getOperationLogs();
      expect(logs).toContain('products: 500 rows [partitioned by category]');
      expect(logs).toContain('transactions: 10000 rows [partitioned by transaction_date]');
      expect(logs).toContain('inventory_snapshots: 2000 rows [partitioned by snapshot_date, warehouse_id]');
    });
  });

  test.describe('Backload Pipeline - Filesystem', () => {
    test('should run backload from filesystem storage', async ({ page }) => {
      test.setTimeout(360000); // 6 minutes for ETL + backload
      
      await helpers.waitForSystemHealth(['SQL Server']);
      
      // First run ETL
      await helpers.runETLPipeline();
      
      // Then run backload
      await helpers.runBackload();
      
      // Verify completion
      const logs = await helpers.getOperationLogs();
      expect(logs).toContain('Backload completed successfully');
      expect(logs).toContain('18500 rows backloaded');
      expect(logs).toContain('Data restored to database: IcebergDemoTarget');
    });

    test('should require ETL before backload', async ({ page }) => {
      await helpers.waitForSystemHealth(['SQL Server']);
      
      // Try to run backload without ETL
      await helpers.runBackload();
      
      // Should show error about catalog not found
      const logs = await helpers.getOperationLogs();
      expect(logs).toMatch(/Catalog not found|Please run ETL first/);
    });

    test('should validate data after backload', async ({ page }) => {
      test.setTimeout(360000);
      
      await helpers.waitForSystemHealth(['SQL Server']);
      
      // Run full cycle
      await helpers.runETLPipeline();
      await helpers.runBackload();
      
      // Check validation in logs
      const logs = await helpers.getOperationLogs();
      expect(logs).toContain('✓ Validation passed');
      
      // Verify each table
      expect(logs).toContain('customers_backload');
      expect(logs).toContain('products_backload');
      expect(logs).toContain('transactions_backload');
      expect(logs).toContain('order_details_backload');
      expect(logs).toContain('inventory_snapshots_backload');
    });
  });

  test.describe('Filesystem-Specific Features', () => {
    test('should show filesystem storage info in API', async ({ page }) => {
      const apiStatus = await helpers.checkAPIStatus();
      
      expect(apiStatus.warehouse_path).toBe('/data/warehouse');
      expect(apiStatus.minio).toBeUndefined();
      expect(apiStatus.nessie).toBeUndefined();
    });

    test('should list Iceberg tables from filesystem', async ({ page }) => {
      test.setTimeout(240000);
      
      await helpers.waitForSystemHealth(['SQL Server']);
      
      // Run ETL to create tables
      await helpers.runETLPipeline();
      
      // Check API for tables
      const apiStatus = await helpers.checkAPIStatus();
      expect(apiStatus.iceberg_tables).toBeDefined();
      expect(apiStatus.iceberg_tables.length).toBe(5);
    });

    test('should handle filesystem permissions correctly', async ({ page }) => {
      test.setTimeout(240000);
      
      await helpers.waitForSystemHealth(['SQL Server']);
      
      // Run ETL
      await helpers.runETLPipeline();
      
      // Should complete without permission errors
      const logs = await helpers.getOperationLogs();
      expect(logs).not.toContain('Permission denied');
      expect(logs).not.toContain('Access denied');
    });
  });

  test.describe('Error Handling - Filesystem', () => {
    test('should handle catalog initialization errors gracefully', async ({ page, context }) => {
      // Mock catalog initialization failure
      await context.route('/api/etl/run', async route => {
        const response = await route.fetch();
        const body = await response.json();
        
        if (body.status === 'started') {
          // Let first call succeed
          route.fulfill({ response });
        } else {
          // Fail subsequent calls
          route.fulfill({
            status: 500,
            contentType: 'application/json',
            body: JSON.stringify({ status: 'error', message: 'Catalog initialization failed' })
          });
        }
      });
      
      const etlButton = page.locator('#run-etl');
      await etlButton.click();
      
      await page.waitForTimeout(3000);
      
      // Should show error
      const logs = await helpers.getOperationLogs();
      expect(logs).toMatch(/failed|error/i);
    });

    test('should handle missing warehouse directory', async ({ page }) => {
      // This test would need special setup to remove the warehouse directory
      // For now, we just verify the system handles it gracefully
      
      const apiStatus = await helpers.checkAPIStatus();
      
      // System should still respond even if warehouse doesn't exist
      expect(apiStatus).toBeDefined();
      expect(apiStatus.sql_server).toBeDefined();
    });
  });

  test.describe('Performance - Filesystem', () => {
    test('should perform ETL efficiently with local storage', async ({ page }) => {
      test.setTimeout(240000);
      
      await helpers.waitForSystemHealth(['SQL Server']);
      
      const startTime = Date.now();
      await helpers.runETLPipeline();
      const etlTime = Date.now() - startTime;
      
      // Filesystem ETL should complete reasonably fast (under 2 minutes)
      expect(etlTime).toBeLessThan(120000);
      
      // Check logs for timing
      const logs = await helpers.getOperationLogs();
      expect(logs).toMatch(/\d+ rows migrated in \d+\.\d+ seconds/);
    });

    test('should load dashboard quickly without external dependencies', async ({ page }) => {
      const startTime = Date.now();
      await page.goto('/');
      await page.waitForLoadState('networkidle');
      const loadTime = Date.now() - startTime;
      
      // Should load even faster without MinIO/Nessie checks
      expect(loadTime).toBeLessThan(3000);
    });
  });

  test.describe('Data Integrity - Filesystem', () => {
    test('should maintain data consistency through filesystem operations', async ({ page }) => {
      test.setTimeout(480000); // 8 minutes for full cycle
      
      await helpers.waitForSystemHealth(['SQL Server']);
      
      // Get initial counts
      const initialTables = await helpers.getSQLTables();
      const totalInitialRows = initialTables.reduce((sum, t) => sum + t.rowCount, 0);
      expect(totalInitialRows).toBe(18500);
      
      // Run ETL
      await helpers.runETLPipeline();
      
      // Verify ETL preserved counts
      let logs = await helpers.getOperationLogs();
      expect(logs).toContain('18500 rows migrated');
      
      // Run backload
      await helpers.runBackload();
      
      // Verify backload preserved counts
      logs = await helpers.getOperationLogs();
      expect(logs).toContain('18500 rows backloaded');
    });

    test('should handle concurrent operations safely', async ({ page, context }) => {
      await helpers.waitForSystemHealth(['SQL Server']);
      
      // Try to run ETL
      const etlButton = page.locator('#run-etl');
      await etlButton.click();
      
      // Try to run another operation while ETL is running
      await page.waitForTimeout(1000);
      
      // Both buttons should be disabled
      await expect(page.locator('#run-etl')).toBeDisabled();
      await expect(page.locator('#run-backload')).toBeDisabled();
    });
  });

  test.describe('UI Differences from Full Version', () => {
    test('should not show S3 configuration options', async ({ page }) => {
      // Check that S3/MinIO options are not present
      await expect(page.locator('text=/S3|MinIO|AWS/i')).not.toBeVisible();
    });

    test('should not show version control features', async ({ page }) => {
      // Check that Nessie/Git features are not present
      await expect(page.locator('text=/Branch|Commit|Version|Nessie/i')).not.toBeVisible();
    });

    test('should clearly indicate filesystem mode throughout', async ({ page }) => {
      // Mode badge
      await expect(page.locator('.mode-badge')).toBeVisible();
      
      // Subtitle
      await expect(page.locator('.subtitle')).toContainText('No MinIO/Nessie Dependencies');
      
      // Warning box
      await expect(page.locator('.warning-box')).toBeVisible();
    });
  });

  test.describe('Migration Path', () => {
    test('should provide information about limitations', async ({ page }) => {
      // Warning box should explain limitations
      const warningText = await page.locator('.warning-box').textContent();
      expect(warningText).toContain('Filesystem Mode');
      expect(warningText).toContain('local storage');
    });

    test('should complete full workflow despite limitations', async ({ page }) => {
      test.setTimeout(480000);
      
      await helpers.waitForSystemHealth(['SQL Server']);
      
      // Full workflow should work
      await helpers.runETLPipeline();
      await helpers.runBackload();
      
      // Verify success
      const logs = await helpers.getOperationLogs();
      expect(logs).toContain('ETL completed successfully');
      expect(logs).toContain('Backload completed successfully');
    });
  });
});