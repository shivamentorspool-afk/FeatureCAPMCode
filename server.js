/**
 * Application Entry Point
 * Validates environment, then starts the CDS server
 */

'use strict';

require('dotenv').config();

const cds = require('@sap/cds');
const fs  = require('fs');

if (!fs.existsSync('./logs')) fs.mkdirSync('./logs', { recursive: true });

const REQUIRED = [
    'OAUTH_CLIENT_ID',
    'OAUTH_CLIENT_SECRET',
    'OAUTH_TOKEN_URL',
    'DATASPHERE_BASE_URL',
    'DATASPHERE_API_PATH'
];

const missing = REQUIRED.filter(v => !process.env[v]);
if (missing.length) {
    console.error(`[STARTUP] Missing env vars: ${missing.join(', ')}`);
    console.error('[STARTUP] Check your .env file.');
    process.exit(1);
}

console.log('='.repeat(60));
console.log('  SAP Datasphere Feature Analytic — CAPM Application');
console.log('='.repeat(60));
console.log(`  Environment : ${process.env.NODE_ENV || 'development'}`);
console.log(`  Port        : ${process.env.PORT || 4004}`);
console.log(`  Refresh     : every ${(parseInt(process.env.REFRESH_INTERVAL_MS) || 300000) / 60000} minutes`);
console.log(`  Max Records : ${process.env.MAX_RECORDS || 10000}`);
console.log(`  API Mode    : $format=json + @odata.nextLink pagination`);
console.log(`  Update API  : ${process.env.CALM_BASE_URL || process.env.DATASPHERE_BASE_URL} (PATCH)`);
console.log('='.repeat(60));

cds.serve('all').catch(err => {
    console.error(`[STARTUP] CDS failed to start: ${err.message}`);
    process.exit(1);
});
