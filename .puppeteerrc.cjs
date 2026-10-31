const { join } = require('node:path');

// Keep Chrome with the deployed application instead of a build-user home directory.
module.exports = { cacheDirectory: join(__dirname, '.cache', 'puppeteer') };
