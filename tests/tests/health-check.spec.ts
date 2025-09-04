import { test, expect } from '@playwright/test';

/**
 * Basic health check to verify services are accessible
 */
test.describe('Service Health Check', () => {
  test('dashboard should be accessible', async ({ page }) => {
    // Try to access the dashboard
    const response = await page.goto('/', { waitUntil: 'domcontentloaded', timeout: 30000 });
    
    // Check response status
    expect(response?.status()).toBeLessThan(400);
    
    // Check for dashboard title
    await expect(page.locator('h1')).toContainText('Apache Iceberg Demo', { timeout: 10000 });
  });

  test('API status endpoint should respond', async ({ request }) => {
    // Check API endpoint
    const response = await request.get('/api/status', { timeout: 30000 });
    
    expect(response.ok()).toBeTruthy();
    
    const data = await response.json();
    expect(data).toHaveProperty('sql_server');
  });

  test('dashboard should have required elements', async ({ page }) => {
    await page.goto('/', { waitUntil: 'domcontentloaded', timeout: 30000 });
    
    // Check for essential UI elements
    await expect(page.locator('#run-etl')).toBeVisible({ timeout: 10000 });
    await expect(page.locator('#run-backload')).toBeVisible({ timeout: 10000 });
    await expect(page.locator('.card')).toHaveCount(5, { timeout: 10000 });
  });
});