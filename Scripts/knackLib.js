/**
 * LAST UPDATED: 08/04/26
 * KnackLib — Knack API client for Google Apps Script
 * --------------------------------------------------
 * Version: 1.6.0
 * Identifier: KnackLib
 *
 * SUMMARY
 *   A lightweight, production-ready client for the Knack REST API, designed for
 *   Google Apps Script (Sheets, Docs, standalone). Supports both Page/View and
 *   Object endpoints, robust retry with exponential backoff, and optional
 *   in-run caching for schema calls.
 *
 * KEY FEATURES
 *   • Page/View endpoints: getRecords, getAllRecords, getRecord, create/update/delete
 *   • Object endpoints: getObjectRecords, getAllObjectRecords, create/update/delete
 *   • Children via connection field key: getRecordChildren, getAllRecordChildren
 *   • Schema helpers: getApplicationSchema, getObjectSchema (optional cache)
 *   • Resilient networking: retries on 429/5xx, honours Retry-After
 *   • Compact, contextual errors and optional debug logging
 *
 * MINIMUM SETUP (consumer project)
 *   1) Store keys in Script Properties (avoid hard-coding):
 *        const props = PropertiesService.getScriptProperties();
 *        props.setProperty('KNACK_APP_KEY',  '<your app key>');
 *        props.setProperty('KNACK_API_KEY',  '<your REST API key>');
 *   2) Add the library: Services → Libraries → Add by Script ID → Identifier: KnackLib → pick a version.
 *   3) Use it:
 *        const props = PropertiesService.getScriptProperties();
 *        const api = new KnackLib.KnackAPI({
 *            applicationKey: props.getProperty('KNACK_APP_KEY'),
 *            apiKey: props.getProperty('KNACK_API_KEY'),
 *            debug: false
 *        });
 *        const rows = api.getAllRecords('scene_XXXX', 'view_YYYY', { rows: 1000 });
 *        Logger.log('Fetched ' + rows.length + ' rows');
 *
 * PUBLIC API (high level)
 *   new KnackLib.KnackAPI({ applicationKey, apiKey, apiUrlBase?, debug? })
 *   // Page/View
 *   .getRecords(sceneId, viewId, opts?)
 *   .getAllRecords(sceneId, viewId, opts?)
 *   .getRecord(sceneId, viewId, recordId)
 *   .createRecord(sceneId, viewId, data)
 *   .updateRecord(sceneId, viewId, recordId, data)
 *   .deleteRecord(sceneId, viewId, recordId)
 *   .getRecordChildren(sceneId, viewId, recordId, connectionFieldKey, opts?)
 *   .getAllRecordChildren(sceneId, viewId, recordId, connectionFieldKey, opts?)
 *   // Object
 *   .getObjectRecord(objectKey, recordId)
 *   .getObjectRecords(objectKey, opts?)
 *   .getAllObjectRecords(objectKey, opts?)
 *   .createObjectRecord(objectKey, data)
 *   .updateObjectRecord(objectKey, recordId, data)
 *   .deleteObjectRecord(objectKey, recordId)
 *   // Schema
 *   .getApplicationSchema(useCache?)
 *   .getObjectSchema(objectKey, useCache?)
 *   // Utilities
 *   .buildFilters(filters)
 *   .buildSorters(sorters)
 *   .formatConnectedFields(records, connectedFields)
 *
 * OPTIONS (common)
 *   opts = {
 *       page?: number,           // 1-based page index
 *       rows?: number,           // rows per page (default 1000 where applicable)
 *       filters?: {...} | [...], // Knack filter JSON or array (see buildFilters)
 *       sorters?: {...} | [...], // sort descriptors (see buildSorters)
 *       rawResponse?: boolean,   // return full API envelope (default false)
 *       extra?: { [k: string]: string | number | boolean } // passthrough query params
 *   }
 *
 * ERROR HANDLING
 *   • Throws Error with .status, .url, and a truncated .body on HTTP failure.
 *   • Retries 429/5xx with exponential backoff; honours Retry-After where present.
 *   • Enable debug to log request stages and paging summaries.
 *
 * NOTES & LIMITS
 *   • Keep your keys in Script/User Properties. Do not commit secrets.
 *   • Respect Knack API limits; prefer getAll* methods with sensible `rows`.
 *   • Sheets cell limits apply (~10M cells per spreadsheet).
 *
 * VERSIONING
 *   • Semantic versioning recommended.
 *   • When you publish changes: File → Manage versions… → Save New Version,
 *     then bump the library version in consumer projects (or use Dev Mode while iterating).
 *
 * LICENSE / AUTHOR
 *   • Licence: <your licence, e.g. MIT>   • Author: <your name/org>   • © <year>
 *
 * CHANGELOG
 *   • 1.0.0 — Initial release.
 *   • 1.1.0 - Add Get Rate Limit
 *   • 1.3.0 - Improved getAplicationSchema to use correct end point
 *   • 1.4.0 - Fixed Sorters to use correct notaion {field: 'field_xyz, direction: 'asc || desc'}
 *   • 1.5.0 - Added getObjectRecord
 *   • 1.6.0 - Fixed buildSorters error in code
 */

var KnackLib = KnackLib || {};
KnackLib.version = '1.6.0';

/**
 * KnackAPI for Google Apps Script
 * - Page/View and Object endpoints
 * - Exponential backoff on 429/5xx (+ honours Retry-After)
 * - Centralised request/response handling with rich errors
 * - Optional in-run caching for schema calls
 *
 * Usage (in a consumer project):
 *   const props = PropertiesService.getScriptProperties();
 *   const api = new KnackLib.KnackAPI({
 *       apiKey: props.getProperty('KNACK_API_KEY'),
 *       applicationKey: props.getProperty('KNACK_APP_KEY'),
 *       debug: false
 *   });
 */
KnackLib.InternalKnackAPI = class {
    constructor(options) {
        const o = options || {};
        this.options = {
            apiKey: o.apiKey,
            applicationKey: o.applicationKey,
            apiUrlBase: o.apiUrlBase || 'https://api.knack.com/v1',
            debug: !!o.debug
        };
        if (!this.options.apiKey || !this.options.applicationKey) {
            throw new Error('KnackAPI: apiKey and applicationKey are required.');
        }
    }

    /* =========================
     * Public: Page/View endpoints
     * ========================= */

    getRecords(sceneId, viewId, options) {
        const params = this._buildParams(options);
        const url = this._formatViewUrl(sceneId, viewId) + this._formatParams(params);
        this._log('getRecords', url);
        const data = this._request(url, { method: 'get' });
        return options && options.rawResponse ? data : data.records;
    }

    getAllRecords(sceneId, viewId, options) {
        const o = options || {};
        const rows = o.rows || 1000;
        let page = 1;
        let all = [];
        let totalPages = 1;

        do {
            const resp = this.getRecords(sceneId, viewId, Object.assign({}, o, {
                page: page,
                rows: rows,
                rawResponse: true
            }));
            if (page === 1) {
                totalPages = resp.total_pages || 1;
                this._log('getAllRecords: paging', { totalPages: totalPages, total_records: resp.total_records });
            }
            if (resp.records && resp.records.length) all = all.concat(resp.records);
            page++;
        } while (page <= totalPages);

        return all;
    }

    getRecord(sceneId, viewId, recordId) {
        if (!sceneId || !viewId || !recordId) {
            throw new Error('getRecord requires sceneId, viewId, and recordId.');
        }
        const url = this._formatViewUrl(sceneId, viewId, recordId);
        this._log('getRecord', url);
        return this._request(url, { method: 'get' });
    }

    createRecord(sceneId, viewId, recordData) {
        const url = this._formatViewUrl(sceneId, viewId);
        this._log('createRecord', { url: url, dataKeys: recordData ? Object.keys(recordData) : [] });
        return this._request(url, {
            method: 'post',
            payload: JSON.stringify(recordData),
            contentType: 'application/json'
        });
    }

    updateRecord(sceneId, viewId, recordId, recordData) {
        const url = this._formatViewUrl(sceneId, viewId, recordId);
        this._log('updateRecord', { url: url, dataKeys: recordData ? Object.keys(recordData) : [] });
        return this._request(url, {
            method: 'put',
            payload: JSON.stringify(recordData),
            contentType: 'application/json'
        });
    }

    deleteRecord(sceneId, viewId, recordId) {
        const url = this._formatViewUrl(sceneId, viewId, recordId);
        this._log('deleteRecord', url);
        return this._request(url, { method: 'delete' });
    }

    /** Child records via connection field key. */
    getRecordChildren(sceneId, viewId, recordId, connectionFieldKey, options) {
        const o = options || {};
        const params = this._buildParams(o);
        params[String(connectionFieldKey) + '_id'] = recordId;

        const url = this._formatViewUrl(sceneId, viewId) + this._formatParams(params);
        this._log('getRecordChildren', url);

        const data = this._request(url, { method: 'get' });
        return o.rawResponse ? data : data.records;
    }

    getAllRecordChildren(sceneId, viewId, recordId, connectionFieldKey, options) {
        const o = options || {};
        const rows = o.rows || 1000;
        let page = 1;
        let all = [];
        let totalPages = 1;

        do {
            const resp = this.getRecordChildren(
                sceneId, viewId, recordId, connectionFieldKey,
                Object.assign({}, o, { page: page, rows: rows, rawResponse: true })
            );
            if (page === 1) {
                totalPages = resp.total_pages || 1;
                this._log('getAllRecordChildren: paging', { totalPages: totalPages, total_records: resp.total_records });
            }
            if (resp.records && resp.records.length) all = all.concat(resp.records);
            page++;
        } while (page <= totalPages);

        return all;
    }

    /* =========================
     * Public: Object endpoints
     * ========================= */

    getObjectRecord(objectKey, recordId) {
        const url = this._formatObjectUrl(objectKey, recordId);
        this._log('getObjectRecords', url);
        return this._request(url, { method: 'get' });
    }

    getObjectRecords(objectKey, options) {
        const params = this._buildParams(options);
        const url = this._formatObjectUrl(objectKey) + this._formatParams(params);
        this._log('getObjectRecords', url);
        const data = this._request(url, { method: 'get' });
        return options && options.rawResponse ? data : data.records;
    }

    getAllObjectRecords(objectKey, options) {
        const o = options || {};
        const rows = o.rows || 1000;
        let page = 1;
        let all = [];
        let totalPages = 1;

        do {
            const resp = this.getObjectRecords(objectKey, Object.assign({}, o, {
                page: page,
                rows: rows,
                rawResponse: true
            }));
            if (page === 1) {
                totalPages = resp.total_pages || 1;
                this._log('getAllObjectRecords: paging', { totalPages: totalPages, total_records: resp.total_records });
            }
            if (resp.records && resp.records.length) all = all.concat(resp.records);
            page++;
        } while (page <= totalPages);

        return all;
    }

    createObjectRecord(objectKey, recordData) {
        const url = this._formatObjectUrl(objectKey);
        this._log('createObjectRecord', { url: url, dataKeys: recordData ? Object.keys(recordData) : [] });
        return this._request(url, {
            method: 'post',
            payload: JSON.stringify(recordData),
            contentType: 'application/json'
        });
    }

    updateObjectRecord(objectKey, recordId, recordData) {
        const url = this._formatObjectUrl(objectKey, recordId);
        this._log('updateObjectRecord', { url: url, dataKeys: recordData ? Object.keys(recordData) : [] });
        return this._request(url, {
            method: 'put',
            payload: JSON.stringify(recordData),
            contentType: 'application/json'
        });
    }

    deleteObjectRecord(objectKey, recordId) {
        const url = this._formatObjectUrl(objectKey, recordId);
        this._log('deleteObjectRecord', url);
        return this._request(url, { method: 'delete' });
    }

    /* =========================
     * Public: Schema helpers
     * ========================= */

    getApplicationSchema(useCache) {
        const cache = useCache ? CacheService.getScriptCache() : null;
        const cacheKey = 'knack_app_schema';
        if (cache) {
            const hit = cache.get(cacheKey);
            if (hit) return JSON.parse(hit);
        }
          // Preferred endpoint: /v1/applications/{APP_ID}
        const appId = this.options.applicationKey; // this is your App ID
        const base  = this.options.apiUrlBase || 'https://api.knack.com/v1';
        const url     = base + '/applications/' + encodeURIComponent(appId);
        this._log('getApplicationSchema', url);
        const data = this._request(url, { method: 'get' });
        if (cache) {
            try { cache.put(cacheKey, JSON.stringify(data), 60); } catch (e) {}
        }
        return data;
    }

    getObjectSchema(objectKey, useCache) {
        const cache = useCache ? CacheService.getScriptCache() : null;
        const cacheKey = 'knack_obj_schema_' + objectKey;
        if (cache) {
            const hit = cache.get(cacheKey);
            if (hit) return JSON.parse(hit);
        }
        const url = this.options.apiUrlBase + '/objects/' + encodeURIComponent(objectKey);
        this._log('getObjectSchema', url);
        const data = this._request(url, { method: 'get' });
        if (cache) {
            try { cache.put(cacheKey, JSON.stringify(data), 60); } catch (e) {}
        }
        return data;
    }

    /**
     * Get current Knack plan limit headers.
     * @param {string=} objectKey  Optional object to hit for a very small request ("object_1" etc).
     * @returns {{remaining:number, limit:number, reset:number, status:number, headers:Object}}
     */
    getRateLimit(objectKey) {
        // Use a very cheap endpoint; /application also works but is heavier.
        let url;
        if (objectKey) {
            url = this.options.apiUrlBase + '/objects/' + encodeURIComponent(objectKey) + '/records?page=1&rows_per_page=1';
        } else {
            url = this.options.apiUrlBase + '/application';
        }

        const fetchOpts = {
            method: 'get',
            headers: this._buildHeaders(),
            muteHttpExceptions: true,
            followRedirects: true,
            validateHttpsCertificates: true
        };

        const resp   = UrlFetchApp.fetch(url, fetchOpts);
        const status = resp.getResponseCode();
        const hdrs   = resp.getAllHeaders() || {};

        // Normalise header keys to lowercase for easy lookup
        const h = {};
        Object.keys(hdrs).forEach(function (k) { h[String(k).toLowerCase()] = hdrs[k]; });

        // Knack typically uses X-PlanLimit-*, but be tolerant
        const remainingStr = h['x-planlimit-remaining'] || h['x-rate-limit-remaining'] || '0';
        const limitStr     = h['x-planlimit-limit']     || h['x-rate-limit-limit']     || '0';
        const resetStr     = h['x-planlimit-reset']     || h['x-rate-limit-reset']     || '0';

        const remaining = parseInt(remainingStr, 10) || 0;
        const limit     = parseInt(limitStr, 10)     || 0;
        const reset     = parseInt(resetStr, 10)     || 0;

        if (this.options.debug) {
            this._log('rate-limit', { status: status, remaining: remaining, limit: limit, reset: reset });
        }

        return { remaining: remaining, limit: limit, reset: reset, status: status, headers: hdrs };
    }


    /* =========================
     * Utilities exposed
     * ========================= */

    formatConnectedFields(records, connectedFields) {
        const arr = Array.isArray(records) ? records : [records];
        return arr.map(function (record) {
            const formatted = Object.assign({}, record);
            (connectedFields || []).forEach(function (field) {
                const raw = record[field + '_raw'];
                if (raw) {
                    formatted[field] = Array.isArray(raw)
                        ? raw.map(function (x) { return Object.assign({}, x); })
                        : Object.assign({}, raw);
                }
            });
            return formatted;
        });
    }

    buildFilters(filters) {
        if (!filters) return {};
        if (filters.match && filters.rules) {
            return { filters: JSON.stringify(filters) };
        }
        const list = Array.isArray(filters) ? filters : [filters];
        const out = {};
        for (let i = 0; i < list.length; i++) {
            const f = list[i];
            const base = 'filters[' + i + ']';
            if (f.field) out[base + '[field]'] = f.field;
            if (f.operator) out[base + '[operator]'] = f.operator;
            if (f.type) out[base + '[type]'] = f.type;
            if (f.value !== undefined) {
                if (Array.isArray(f.value)) {
                    for (let v = 0; v < f.value.length; v++) {
                        out[base + '[value][' + v + ']'] = f.value[v];
                    }
                } else {
                    out[base + '[value]'] = f.value;
                }
            }
        }
        return out;
    }

    /**
     * Build Knack v1 API sort parameters.
     * Accepts a single sorter { field: 'field_25', direction: 'asc'|'desc' }
     * or an array and uses the first item (Knack v1 doesn't support multi-sort).
     *
     * @param {{field:string, direction?:'asc'|'desc'}|Array} sorters
     * @returns {{sort_field:string, sort_order:'asc'|'desc'}|{}}
     */
    buildSorters(sorters) {
        if (!sorters) return {};

        const sortOrder = String(sorters.direction || 'asc').toLowerCase() === 'desc' ? 'desc' : 'asc';

        return {
            sort_field: sorters.field,
            sort_order: sortOrder
        };
    }

    /* =========================
     * Internals
     * ========================= */

    _formatViewUrl(sceneId, viewId, recordId) {
        if (!sceneId || !viewId) {
            throw new Error('sceneId and viewId are required.');
        }
        let url = this.options.apiUrlBase + '/pages/' + encodeURIComponent(sceneId) + '/views/' + encodeURIComponent(viewId);
        url += recordId ? '/records/' + encodeURIComponent(recordId) : '/records';
        return url;
    }

    _formatObjectUrl(objectKey, recordId) {
        if (!objectKey) throw new Error('objectKey is required.');
        let url = this.options.apiUrlBase + '/objects/' + encodeURIComponent(objectKey) + '/records';
        if (recordId) url += '/' + encodeURIComponent(recordId);
        return url;
    }

    _buildHeaders() {
        return {
            'Content-Type': 'application/json',
            'X-Knack-Application-Id': this.options.applicationKey,
            'X-Knack-REST-API-Key': this.options.apiKey
        };
    }

    _buildParams(options) {
        const o = options || {};
        let params = {};
        if (o.filters) params = Object.assign(params, this.buildFilters(o.filters));
        if (o.sorters) params = Object.assign(params, this.buildSorters(o.sorters));
        if (o.page != null) params.page = o.page;
        if (o.rows != null) params.rows_per_page = o.rows;
        if (o.extra && typeof o.extra === 'object') {
            Object.keys(o.extra).forEach(function (k) {
                const v = o.extra[k];
                if (v !== undefined && v !== null) params[k] = v;
            });
        }
        return params;
    }

    _formatParams(params) {
        if (!params || Object.keys(params).length === 0) return '';
        const parts = [];
        Object.keys(params).forEach(function (k) {
            parts.push(encodeURIComponent(k) + '=' + encodeURIComponent(String(params[k])));
        });
        return '?' + parts.join('&');
    }

    _request(url, opts) {
        const options = opts || {};
        const method = (options.method || 'get').toLowerCase();

        const fetchOpts = {
            method: method,
            headers: this._buildHeaders(),
            muteHttpExceptions: true,
            followRedirects: true,
            validateHttpsCertificates: true
        };
        if (options.payload != null) fetchOpts.payload = options.payload;
        if (options.contentType) fetchOpts.contentType = options.contentType;

        const maxRetries = 5;
        let attempt = 0;

        while (true) {
            try {
                const resp = UrlFetchApp.fetch(url, fetchOpts);
                const code = resp.getResponseCode();
                const text = resp.getContentText();

                if (code >= 200 && code < 300) {
                    const json = this._safeJson(text);
                    if (this.options.debug) this._log('response(ok)', { code: code, bytes: text ? text.length : 0 });
                    return json;
                }

                if (code === 429 || (code >= 500 && code < 600)) {
                    attempt++;
                    if (attempt > maxRetries) {
                        throw this._makeError('KnackAPI request failed after retries', code, url, text);
                    }
                    const retryAfter = this._retryAfterMs(resp) || this._backoffMs(attempt);
                    this._log('retry ' + attempt + ' in ' + retryAfter + 'ms (code ' + code + ')', { url: url });
                    Utilities.sleep(retryAfter);
                    continue;
                }

                throw this._makeError('KnackAPI request failed', code, url, text);

            } catch (e) {
                attempt++;
                if (attempt > maxRetries) throw e;
                const sleepMs = this._backoffMs(attempt);
                this._log('network error, retry ' + attempt + ' in ' + sleepMs + 'ms', { message: String(e) });
                Utilities.sleep(sleepMs);
            }
        }
    }

    _retryAfterMs(resp) {
        try {
            const headers = resp.getAllHeaders();
            const ra = headers && headers['Retry-After'];
            if (!ra) return 0;
            const num = Number(ra);
            if (!isNaN(num)) return Math.max(0, num * 1000);
        } catch (e) {}
        return 0;
    }

    _backoffMs(attempt) {
        // 500ms * 2^(n-1), jittered, capped at 60s
        const base = 500 * Math.pow(2, Math.max(0, attempt - 1));
        const wait = Math.min(60000, base);
        const jitter = Math.floor(wait * (0.5 + Math.random() * 0.5));
        return jitter;
    }

    _safeJson(text) {
        try { return JSON.parse(text || '{}'); }
        catch (e) { throw new Error('KnackAPI: invalid JSON in response: ' + String(e) + ' body: ' + String(text).slice(0, 500)); }
    }

    _makeError(message, code, url, body) {
        const err = new Error(message + ' (HTTP ' + code + ')');
        err.status = code;
        err.url = url;
        err.body = typeof body === 'string' ? body.slice(0, 2000) : body;
        if (this.options.debug) {
            Logger.log('[KnackAPI error] ' + message + ' code=' + code + ' url=' + url);
            if (body) Logger.log('[KnackAPI error body] ' + String(err.body));
        }
        return err;
    }

    _log(message, data) {
        if (!this.options.debug) return;
        if (data === undefined) {
            Logger.log('[KnackAPI] ' + message);
        } else {
            let payload = data;
            try {
                const s = JSON.stringify(data);
                payload = s && s.length > 1000 ? s.slice(0, 1000) + '…' : s;
            } catch (e) {}
            Logger.log('[KnackAPI] ' + message + ' ' + payload);
        }
    }
};
/**
 * Public constructor for consumers of the library.
 * Using `new KnackLib.KnackAPI(opts)` in a consumer project will call this function.
 * The function returns the real class instance so `new` works as expected.
 *
 * @constructor
 * @param {Object} options
 * @return {Object} instance of the internal ES6 class
 */
function KnackAPI(options) {
  return new KnackLib.InternalKnackAPI(options);
}

/** Optional: expose version as a function (since variables are not exported) */
function getVersion() {
  return KnackLib.version;
}