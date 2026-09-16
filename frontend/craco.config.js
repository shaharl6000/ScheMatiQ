const path = require('path');

module.exports = {
  webpack: {
    alias: {
      '@': path.resolve(__dirname, 'src'),
    },
    configure: (webpackConfig) => {
      // Ensure the alias is properly resolved
      webpackConfig.resolve.alias['@'] = path.resolve(__dirname, 'src');
      return webpackConfig;
    },
  },
  // Webpack aliases are not visible to Jest (a separate resolver), so any
  // source file under test that imports via '@/...' fails with "Cannot find
  // module" unless the same alias is mirrored here.
  jest: {
    configure: (jestConfig) => {
      jestConfig.moduleNameMapper = {
        ...jestConfig.moduleNameMapper,
        '^@/(.*)$': '<rootDir>/src/$1',
      };
      return jestConfig;
    },
  },
};
