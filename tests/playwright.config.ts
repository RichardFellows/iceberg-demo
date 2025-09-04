import { defineConfig, devices } from '@playwright/test';

/**
 * Playwright configuration for Apache Iceberg Demo tests
 */
export default defineConfig({
  testDir: './tests',
  fullyParallel: false, // Run tests sequentially due to shared database state
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1, // Single worker to avoid conflicts
  reporter: process.env.CI 
    ? [['github'], ['html', { open: 'never' }]]
    : [['html', { open: 'on-failure' }]],
  
  use: {
    baseURL: 'http://localhost:8000',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
    actionTimeout: 30000,
  },

  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
    {
      name: 'firefox',
      use: { ...devices['Desktop Firefox'] },
    },
  ],

  // Global setup and teardown
  // Only use local setup if not in CI (CI handles Docker separately)
  globalSetup: process.env.CI ? undefined : './tests/global-setup.ts',
  globalTeardown: process.env.CI ? undefined : './tests/global-teardown.ts',

  // Timeouts
  timeout: 120000, // 2 minutes per test (ETL operations can be slow)
  expect: {
    timeout: 30000, // 30 seconds for assertions
  },

  // Web server configuration - disabled in CI as services are managed by workflow
  webServer: process.env.CI ? undefined : [
    {
      command: 'docker-compose up -d --build',
      port: 8000,
      timeout: 180000, // 3 minutes to start all services
      reuseExistingServer: true,
      env: {
        COMPOSE_FILE: process.env.TEST_MODE === 'filesystem' 
          ? 'docker-compose-filesystem.yml' 
          : 'docker-compose.yml'
      }
    }
  ],
});