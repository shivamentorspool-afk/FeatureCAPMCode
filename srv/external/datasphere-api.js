/**
 * SAP Datasphere API Client (standalone module)
 *
 * ⚠️  API Constraints:
 *     - Only $format=json is supported on the first request.
 *     - $top is rejected with HTTP 400.
 *     - Full dataset is retrieved by following @odata.nextLink pages.
 */

'use strict';

const axios = require('axios');
const qs    = require('qs');

let tokenCache = { accessToken: null, expiresAt: null, tokenType: 'Bearer' };

async function fetchNewToken() {
    const response = await axios.post(
        process.env.OAUTH_TOKEN_URL,
        qs.stringify({
            grant_type:    'client_credentials',
            client_id:     process.env.OAUTH_CLIENT_ID,
            client_secret: process.env.OAUTH_CLIENT_SECRET
        }),
        { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 30000 }
    );
    const { access_token, expires_in, token_type } = response.data;
    tokenCache = {
        accessToken: access_token,
        expiresAt:   new Date(Date.now() + (expires_in - 60) * 1000),
        tokenType:   token_type || 'Bearer'
    };
    return tokenCache.accessToken;
}

async function getValidToken() {
    if (tokenCache.accessToken && new Date() < tokenCache.expiresAt) return tokenCache.accessToken;
    return fetchNewToken();
}

/**
 * Fetch ALL records by following @odata.nextLink pagination.
 * No $top is sent. Only $format=json on the first request.
 *
 * @param {number} maxRecords - Stop accumulating after this many records
 * @returns {Promise<Array>}
 */
async function fetchFeatureAnalyticData(maxRecords = 10000) {
    const firstUrl = `${process.env.DATASPHERE_BASE_URL}${process.env.DATASPHERE_API_PATH}`;
    const token    = await getValidToken();

    let allRecords = [];
    let currentUrl = `${firstUrl}?$format=json`;
    let page       = 0;

    while (currentUrl && allRecords.length < maxRecords) {
        page++;
        const res      = await axios.get(currentUrl, {
            headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/json' },
            timeout: 120000
        });
        const records  = res.data?.value || [];
        const nextLink = res.data?.['@odata.nextLink'] || null;

        allRecords = allRecords.concat(records);
        console.log(`[datasphere-api] Page ${page}: ${records.length} records | nextLink: ${nextLink ? 'YES' : 'NO'}`);

        currentUrl = nextLink;
    }

    return allRecords.slice(0, maxRecords);
}

/**
 * PATCH feature status via SAP Cloud ALM API.
 *
 * @param {string} featureId  - Feature ID
 * @param {string} projectId  - Project ID (UUID)
 * @param {string} newStatus  - New status code
 * @returns {Promise<object>}
 */
async function patchFeatureStatus(featureId, projectId, newStatus) {
    const token = await getValidToken();

    const calmBase    = process.env.CALM_BASE_URL;
    const calmApiPath = process.env.CALM_FEATURE_API_PATH;

    let patchUrl;

    if (calmBase && calmApiPath) {
        patchUrl = `${calmBase}${calmApiPath}`
            .replace('{projectId}', encodeURIComponent(projectId))
            .replace('{featureId}', encodeURIComponent(featureId));
    } else {
        const dsBase    = process.env.DATASPHERE_BASE_URL;
        const dsApiPath = process.env.DATASPHERE_API_PATH;
        patchUrl = `${dsBase}${dsApiPath}(featureId='${encodeURIComponent(featureId)}')`;
    }

    const response = await axios.patch(
        patchUrl,
        { status: newStatus },
        {
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type':  'application/json',
                'Accept':        'application/json'
            },
            timeout: 60000
        }
    );

    return response.data;
}

module.exports = { fetchFeatureAnalyticData, patchFeatureStatus, getValidToken, fetchNewToken };
