module.exports = {
  // The test environment that will be used for testing
  testEnvironment: 'node',
  
  // The glob patterns Jest uses to detect test files
  testMatch: [
    '**/tests/**/*.test.js',
    '**/tests/**/*.spec.js'
  ],
  
  // An array of regexp pattern strings that are matched against all test paths
  // matched tests are skipped
  testPathIgnorePatterns: [
    '/node_modules/'
  ],
  
  // An array of regexp pattern strings that are matched against all source file paths
  // matched files will have coverage information generated
  collectCoverageFrom: [
    'src/**/*.js',
    '!src/reports/**',
    '!**/node_modules/**'
  ],
  
  // The directory where Jest should output its coverage files
  coverageDirectory: 'coverage',
  
  // Indicates whether each individual test should be reported during the run
  verbose: true,
  
  // Automatically clear mock calls and instances between every test
  clearMocks: true,
  
  // Indicates whether the coverage information should be collected while executing the test
  collectCoverage: false,
  
  // A list of reporter names that Jest uses when writing coverage reports
  coverageReporters: [
    'json',
    'text',
    'lcov',
    'clover'
  ],
  
  // The maximum amount of workers used to run your tests
  maxWorkers: '50%',
  
  // An array of directory names to be searched recursively up from the requiring module's location
  moduleDirectories: [
    'node_modules',
    'src'
  ],
  
  // A map from regular expressions to module names that allow to stub out resources
  moduleNameMapper: {
    '^@/(.*)$': '<rootDir>/src/$1'
  }
}; 