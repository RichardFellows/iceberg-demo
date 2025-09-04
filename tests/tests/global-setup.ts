import { FullConfig } from '@playwright/test';
import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Global setup for Playwright tests
 * Ensures Docker services are ready before tests begin
 */
async function globalSetup(config: FullConfig) {
  console.log('\n🚀 Starting global setup for Apache Iceberg Demo tests...\n');
  
  const testMode = process.env.TEST_MODE || 'full';
  const composeFile = testMode === 'filesystem' 
    ? 'docker-compose-filesystem.yml' 
    : 'docker-compose.yml';
  
  console.log(`📋 Test mode: ${testMode}`);
  console.log(`📄 Using compose file: ${composeFile}\n`);
  
  try {
    // Ensure data directories exist
    const dataDir = path.join(process.cwd(), 'data', 'warehouse');
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
      console.log(`✅ Created data directory: ${dataDir}`);
    }
    
    // Check if services are already running
    try {
      const psOutput = execSync(`docker-compose -f ${composeFile} ps --services --filter "status=running"`, {
        encoding: 'utf-8',
        stdio: 'pipe'
      });
      
      if (psOutput.trim()) {
        console.log('✅ Services already running:', psOutput.trim().split('\n').join(', '));
        
        // Wait a bit for services to be fully ready
        await waitForServices(testMode);
        return;
      }
    } catch {
      // Services not running, continue with startup
    }
    
    // Stop any existing containers
    console.log('🛑 Stopping any existing containers...');
    execSync(`docker-compose -f ${composeFile} down`, {
      stdio: 'inherit'
    });
    
    // Start services
    console.log('\n🐳 Starting Docker services...');
    execSync(`docker-compose -f ${composeFile} up -d --build`, {
      stdio: 'inherit'
    });
    
    // Wait for services to be healthy
    await waitForServices(testMode);
    
    console.log('\n✅ Global setup complete!\n');
    
  } catch (error) {
    console.error('\n❌ Global setup failed:', error);
    
    // Try to show container logs for debugging
    try {
      console.log('\n📋 Container logs:');
      execSync(`docker-compose -f ${composeFile} logs --tail=50`, {
        stdio: 'inherit'
      });
    } catch {
      // Ignore errors getting logs
    }
    
    throw error;
  }
}

/**
 * Wait for services to be healthy
 */
async function waitForServices(testMode: string) {
  const composeFile = testMode === 'filesystem' 
    ? 'docker-compose-filesystem.yml' 
    : 'docker-compose.yml';
  
  console.log('\n⏳ Waiting for services to be healthy...');
  
  const maxRetries = 60; // 3 minutes (60 * 3 seconds)
  let retries = 0;
  
  while (retries < maxRetries) {
    try {
      // Check SQL Server health
      const sqlHealthy = await checkServiceHealth(composeFile, 'mssql');
      
      if (testMode === 'full') {
        // For full version, also check Nessie and MinIO
        const nessieHealthy = await checkServiceHealth(composeFile, 'nessie');
        const minioHealthy = await checkServiceHealth(composeFile, 'minio');
        
        if (sqlHealthy && nessieHealthy && minioHealthy) {
          console.log('✅ All services are healthy!');
          break;
        }
      } else {
        // For filesystem version, only SQL Server is required
        if (sqlHealthy) {
          console.log('✅ SQL Server is healthy!');
          break;
        }
      }
      
    } catch (error) {
      // Service not healthy yet
    }
    
    retries++;
    if (retries >= maxRetries) {
      throw new Error('Services did not become healthy in time');
    }
    
    // Show progress
    if (retries % 10 === 0) {
      console.log(`   Still waiting... (${retries * 3} seconds elapsed)`);
    }
    
    // Wait 3 seconds before retry
    await new Promise(resolve => setTimeout(resolve, 3000));
  }
  
  // Additional wait for services to stabilize
  console.log('   Waiting for services to stabilize...');
  await new Promise(resolve => setTimeout(resolve, 5000));
}

/**
 * Check if a specific service is healthy
 */
async function checkServiceHealth(composeFile: string, serviceName: string): Promise<boolean> {
  try {
    const output = execSync(
      `docker-compose -f ${composeFile} ps ${serviceName} | grep -E "\\(healthy\\)|Up.*healthy"`,
      { encoding: 'utf-8', stdio: 'pipe' }
    );
    
    return output.includes('healthy');
  } catch {
    return false;
  }
}

export default globalSetup;