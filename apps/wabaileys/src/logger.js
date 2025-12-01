const pino = require('pino');
const config = require('./config');

const logger = pino({
  level: config.logLevel || 'info',
  base: { service: 'wabaileys' },
  timestamp: pino.stdTimeFunctions.isoTime,
});

module.exports = logger;
