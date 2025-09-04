import { FullConfig } from '@playwright/test';
import { execSync } from 'child_process';

/**
 * Global teardown for Playwright tests
 * Optionally stops Docker services after tests complete
 */
async function globalTeardown(config: FullConfig) {
  console.log('\n🧹 Running global teardown...\n');
  
  const testMode = process.env.TEST_MODE || 'full';
  const composeFile = testMode === 'filesystem' 
    ? 'docker-compose-filesystem.yml' 
    : 'docker-compose.yml';
  
  // Only stop services in CI or if explicitly requested
  if (process.env.CI || process.env.STOP_AFTER_TEST === 'true') {
    console.log('🛑 Stopping Docker services...');
    
    try {
      execSync(`docker-compose -f ${composeFile} down`, {
        stdio: 'inherit'
      });
      console.log('✅ Services stopped successfully');
    } catch (error) {
      console.error('❌ Failed to stop services:', error);
    }
  } else {
    console.log('ℹ️  Leaving services running for debugging');
    console.log(`   To stop manually: docker-compose -f ${composeFile} down`);
  }
  
  console.log('\n✅ Global teardown complete!\n');
}

export default globalTeardown;