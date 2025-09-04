import { Page, expect } from '@playwright/test';

/**
 * Test helper utilities for Apache Iceberg Demo
 */

export class DashboardHelpers {
  constructor(private page: Page) {}

  /**
   * Wait for system to be healthy
   */
  async waitForSystemHealth(requiredServices: string[] = ['SQL Server']) {
    await this.page.waitForLoadState('networkidle');
    
    for (const service of requiredServices) {
      await expect(
        this.page.locator('.status').filter({ hasText: service })
      ).toContainText(/Connected|Ready|Healthy|Initialized/i, { timeout: 60000 });
    }
  }

  /**
   * Get system status for a specific service
   */
  async getServiceStatus(serviceName: string): Promise<string> {
    const statusElement = await this.page
      .locator('.status')
      .filter({ hasText: serviceName })
      .first();
    
    const statusText = await statusElement.textContent();
    return statusText || '';
  }

  /**
   * Run ETL pipeline and wait for completion
   */
  async runETLPipeline() {
    // Click ETL button
    const etlButton = this.page.locator('#run-etl');
    await expect(etlButton).toBeEnabled({ timeout: 10000 });
    await etlButton.click();

    // Wait for progress container to appear
    await expect(this.page.locator('#progress-container')).toBeVisible();

    // Wait for completion (up to 3 minutes)
    await this.waitForJobCompletion('etl', 180000);
  }

  /**
   * Run backload and wait for completion
   */
  async runBackload() {
    // Click backload button
    const backloadButton = this.page.locator('#run-backload');
    await expect(backloadButton).toBeEnabled({ timeout: 10000 });
    await backloadButton.click();

    // Wait for progress container to appear
    await expect(this.page.locator('#progress-container')).toBeVisible();

    // Wait for completion (up to 3 minutes)
    await this.waitForJobCompletion('backload', 180000);
  }

  /**
   * Wait for a job to complete
   */
  async waitForJobCompletion(jobType: 'etl' | 'backload', timeout: number = 120000) {
    const startTime = Date.now();
    
    while (Date.now() - startTime < timeout) {
      // Check progress bar
      const progressBar = this.page.locator('#progress-bar');
      const progressText = await progressBar.textContent();
      
      if (progressText === '100%') {
        // Verify completion in logs
        const logContent = await this.page.locator('#log-viewer').textContent();
        if (logContent?.includes(`✓ ${jobType.toUpperCase()} completed successfully`)) {
          return;
        }
      }

      // Check for failure
      const logContent = await this.page.locator('#log-viewer').textContent();
      if (logContent?.includes(`✗ ${jobType.toUpperCase()} failed`)) {
        throw new Error(`${jobType} job failed: ${logContent}`);
      }

      // Wait before next check
      await this.page.waitForTimeout(2000);
    }

    throw new Error(`${jobType} job did not complete within ${timeout}ms`);
  }

  /**
   * Get SQL table information
   */
  async getSQLTables(): Promise<Array<{name: string, rowCount: number}>> {
    await this.page.waitForSelector('.table-item', { timeout: 10000 });
    
    const tables = await this.page.locator('#sql-tables .table-item').all();
    const tableInfo = [];
    
    for (const table of tables) {
      const name = await table.locator('span:first-child').textContent();
      const rowCountText = await table.locator('.row-count').textContent();
      const rowCount = parseInt(rowCountText?.replace(/[^\d]/g, '') || '0');
      
      if (name) {
        tableInfo.push({ name, rowCount });
      }
    }
    
    return tableInfo;
  }

  /**
   * Get Iceberg table information
   */
  async getIcebergTables(): Promise<string[]> {
    const tablesContainer = this.page.locator('#iceberg-tables');
    
    // Check if no tables message is shown
    const noTablesMessage = await tablesContainer.textContent();
    if (noTablesMessage?.includes('No tables yet')) {
      return [];
    }
    
    const tables = await tablesContainer.locator('.table-item').all();
    const tableNames = [];
    
    for (const table of tables) {
      const name = await table.textContent();
      if (name) {
        tableNames.push(name.trim());
      }
    }
    
    return tableNames;
  }

  /**
   * Get Nessie catalog tables (for full version)
   */
  async getNessieTables(): Promise<string[]> {
    const tablesContainer = this.page.locator('#nessie-tables');
    
    // Check if container exists (only in full version)
    const exists = await tablesContainer.count() > 0;
    if (!exists) {
      return [];
    }
    
    const tables = await tablesContainer.locator('.table-item').all();
    const tableNames = [];
    
    for (const table of tables) {
      const name = await table.textContent();
      if (name) {
        tableNames.push(name.trim());
      }
    }
    
    return tableNames;
  }

  /**
   * Get operation logs
   */
  async getOperationLogs(): Promise<string> {
    const logViewer = this.page.locator('#log-viewer');
    return await logViewer.textContent() || '';
  }

  /**
   * Clear operation logs (if applicable)
   */
  async clearLogs() {
    // Some dashboards may have a clear button
    const clearButton = this.page.locator('#clear-logs');
    if (await clearButton.count() > 0) {
      await clearButton.click();
    }
  }

  /**
   * Check if a service is healthy
   */
  async isServiceHealthy(serviceName: string): Promise<boolean> {
    const status = await this.getServiceStatus(serviceName);
    return /Connected|Ready|Healthy|Initialized/i.test(status);
  }

  /**
   * Wait for specific log message
   */
  async waitForLogMessage(message: string, timeout: number = 30000) {
    const startTime = Date.now();
    
    while (Date.now() - startTime < timeout) {
      const logs = await this.getOperationLogs();
      if (logs.includes(message)) {
        return;
      }
      await this.page.waitForTimeout(1000);
    }
    
    throw new Error(`Log message "${message}" not found within ${timeout}ms`);
  }

  /**
   * Take a screenshot with a descriptive name
   */
  async takeScreenshot(name: string) {
    await this.page.screenshot({ 
      path: `tests/screenshots/${name}-${Date.now()}.png`,
      fullPage: true 
    });
  }

  /**
   * Check API status endpoint
   */
  async checkAPIStatus(): Promise<any> {
    const response = await this.page.request.get('/api/status');
    expect(response.ok()).toBeTruthy();
    return await response.json();
  }

  /**
   * Wait for ETL button to be enabled
   */
  async waitForETLReady() {
    await expect(this.page.locator('#run-etl')).toBeEnabled({ timeout: 30000 });
  }

  /**
   * Wait for Backload button to be enabled
   */
  async waitForBackloadReady() {
    await expect(this.page.locator('#run-backload')).toBeEnabled({ timeout: 30000 });
  }

  /**
   * Get progress percentage
   */
  async getProgress(): Promise<number> {
    const progressBar = this.page.locator('#progress-bar');
    const progressText = await progressBar.textContent();
    return parseInt(progressText?.replace('%', '') || '0');
  }

  /**
   * Verify table row counts match expected values
   */
  async verifyTableCounts(expectedCounts: Record<string, number>) {
    const tables = await this.getSQLTables();
    
    for (const [tableName, expectedCount] of Object.entries(expectedCounts)) {
      const table = tables.find(t => t.name === tableName);
      expect(table, `Table ${tableName} not found`).toBeDefined();
      expect(table?.rowCount, `Row count for ${tableName}`).toBe(expectedCount);
    }
  }

  /**
   * Check if MinIO is configured (for full version)
   */
  async isMinIOConfigured(): Promise<boolean> {
    const status = await this.checkAPIStatus();
    return status.minio !== undefined;
  }

  /**
   * Check if Nessie is configured (for full version)
   */
  async isNessieConfigured(): Promise<boolean> {
    const status = await this.checkAPIStatus();
    return status.nessie !== undefined;
  }
}

/**
 * Retry helper for flaky operations
 */
export async function retryOperation<T>(
  operation: () => Promise<T>,
  maxRetries: number = 3,
  delay: number = 1000
): Promise<T> {
  let lastError;
  
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (i < maxRetries - 1) {
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }
  
  throw lastError;
}

/**
 * Wait helper with custom condition
 */
export async function waitForCondition(
  condition: () => Promise<boolean>,
  timeout: number = 30000,
  interval: number = 1000
): Promise<void> {
  const startTime = Date.now();
  
  while (Date.now() - startTime < timeout) {
    if (await condition()) {
      return;
    }
    await new Promise(resolve => setTimeout(resolve, interval));
  }
  
  throw new Error(`Condition not met within ${timeout}ms`);
}