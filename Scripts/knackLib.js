/* global CacheService, Logger, UrlFetchApp, Utilities */

/**
 * LAST UPDATED: 30/04/26
 * KnackLib — Knack API client for Google Apps Script
 * --------------------------------------------------
 * Version: 1.7.0
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
 *   • Usage/rate-limit stats helpers aligned with knack-functions KnackAPI
 *   • Batch writes with progress callbacks and optional continue-on-error
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
 *   .createRecords(sceneId, viewId, recordsData, opts?)
 *   .updateRecord(sceneId, viewId, recordId, data)
 *   .updateRecords(sceneId, viewId, recordIdsOrOps, dataOrOpts?, opts?)
 *   .deleteRecord(sceneId, viewId, recordId)
 *   .deleteRecords(sceneId, viewId, recordIds, opts?)
 *   .getRecordChildren(sceneId, viewId, recordId, connectionFieldKey, opts?)
 *   .getAllRecordChildren(sceneId, viewId, recordId, connectionFieldKey, opts?)
 *   // Object
 *   .getObjectRecord(objectKey, recordId)
 *   .getObjectRecords(objectKey, opts?)
 *   .getAllObjectRecords(objectKey, opts?)
 *   .createObjectRecord(objectKey, data)
 *   .createObjectRecords(objectKey, recordsData, opts?)
 *   .updateObjectRecord(objectKey, recordId, data)
 *   .updateObjectRecords(objectKey, recordIdsOrOps, dataOrOpts?, opts?)
 *   .deleteObjectRecord(objectKey, recordId)
 *   .deleteObjectRecords(objectKey, recordIds, opts?)
 *   // Schema
 *   .getApplicationSchema(useCache?)
 *   .getObjectSchema(objectKey, useCache?)
 *   // Utilities
 *   .buildFilters(filters)
 *   .buildSorters(sorters)
 *   .formatConnectedFields(records, connectedFields)
 *   .getApiUsageStats()
 *   .getRunApiCallCount()
 *   .getRateLimitStatus()
 *   .resetApiUsageStats(opts?)
 *   .refreshApiUsageStats(objectKey?)
 *   .logApiUsageStats(opts?)
 *
 * OPTIONS (common)
 *   opts = {
 *       page?: number,           // 1-based page index
 *       rows?: number,           // rows per page (default 1000 where applicable)
 *       filters?: {...} | [...], // Knack filter JSON or array (see buildFilters)
 *       sorters?: {...} | [...], // sort descriptors (see buildSorters)
 *       rawResponse?: boolean,   // return full API envelope (default false)
 *       pageConcurrency?: number,// use UrlFetchApp.fetchAll for remaining pages (default 1)
 *       continueOnError?: boolean,// batch writes continue after failures (default false)
 *       onProgress?: Function,   // batch/page progress callback
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
 *   • 1.7.0 - Add usage stats, richer rate-limit snapshots, fetchAll paging, and batch writes
 */

var KnackLib = KnackLib || {};
KnackLib.version = '1.7.0';

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
            debug: !!o.debug,
            maxRetries: Number.isFinite(o.maxRetries) ? Math.max(0, Math.floor(o.maxRetries)) : 5,
            retryDelayBase: Number.isFinite(o.retryDelayBase) ? Math.max(0, Math.floor(o.retryDelayBase)) : 500,
            retryDelayMax: Number.isFinite(o.retryDelayMax) ? Math.max(0, Math.floor(o.retryDelayMax)) : 60000,
            retryOnStatus: Array.isArray(o.retryOnStatus) ? o.retryOnStatus : [429, 500, 502, 503, 504],
            pageConcurrency: Number.isFinite(o.pageConcurrency) ? Math.max(1, Math.floor(o.pageConcurrency)) : 1
        };
        if (!this.options.apiKey || !this.options.applicationKey) {
            throw new Error('KnackAPI: apiKey and applicationKey are required.');
        }
        this._initApiUsageState();
    }

    /* =========================
     * Public: Page/View endpoints
     * ========================= */

    getRecords(sceneId, viewId, options) {
        const params = this._buildParams(options);
        const url = this._formatViewUrl(sceneId, viewId) + this._formatParams(params);
        if (options && options.__urlOnly) return { url: url };
        this._log('getRecords', url);
        const data = this._request(url, { method: 'get' });
        return options && options.rawResponse ? data : data.records;
    }

    getAllRecords(sceneId, viewId, options) {
        return this._getAllPages(function (pageOptions) {
            return this.getRecords(sceneId, viewId, pageOptions);
        }, options, 'getAllRecords');
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

    createRecords(sceneId, viewId, recordsData, options) {
        return this._runBatch(recordsData, options, 'created', function (recordData) {
            return this.createRecord(sceneId, viewId, recordData);
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

    updateRecords(sceneId, viewId, recordIds, recordData, options) {
        const context = this._normalizeUpdateBatch(recordIds, recordData, options);
        return this._runBatch(context.records, context.options, 'updated', function (operation) {
            return this.updateRecord(sceneId, viewId, operation.id, operation.data);
        });
    }

    deleteRecord(sceneId, viewId, recordId) {
        const url = this._formatViewUrl(sceneId, viewId, recordId);
        this._log('deleteRecord', url);
        return this._request(url, { method: 'delete' });
    }

    deleteRecords(sceneId, viewId, recordIds, options) {
        return this._runBatch(recordIds, options, 'deleted', function (recordId) {
            return this.deleteRecord(sceneId, viewId, recordId);
        });
    }

    /** Child records via connection field key. */
    getRecordChildren(sceneId, viewId, recordId, connectionFieldKey, options) {
        const o = options || {};
        const params = this._buildParams(o);
        params[String(connectionFieldKey) + '_id'] = recordId;

        const url = this._formatViewUrl(sceneId, viewId) + this._formatParams(params);
        if (o.__urlOnly) return { url: url };
        this._log('getRecordChildren', url);

        const data = this._request(url, { method: 'get' });
        return o.rawResponse ? data : data.records;
    }

    getAllRecordChildren(sceneId, viewId, recordId, connectionFieldKey, options) {
        return this._getAllPages(function (pageOptions) {
            return this.getRecordChildren(sceneId, viewId, recordId, connectionFieldKey, pageOptions);
        }, options, 'getAllRecordChildren');
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
        if (options && options.__urlOnly) return { url: url };
        this._log('getObjectRecords', url);
        const data = this._request(url, { method: 'get' });
        return options && options.rawResponse ? data : data.records;
    }

    getAllObjectRecords(objectKey, options) {
        return this._getAllPages(function (pageOptions) {
            return this.getObjectRecords(objectKey, pageOptions);
        }, options, 'getAllObjectRecords');
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

    createObjectRecords(objectKey, recordsData, options) {
        return this._runBatch(recordsData, options, 'created', function (recordData) {
            return this.createObjectRecord(objectKey, recordData);
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

    updateObjectRecords(objectKey, recordIds, recordData, options) {
        const context = this._normalizeUpdateBatch(recordIds, recordData, options);
        return this._runBatch(context.records, context.options, 'updated', function (operation) {
            return this.updateObjectRecord(objectKey, operation.id, operation.data);
        });
    }

    deleteObjectRecord(objectKey, recordId) {
        const url = this._formatObjectUrl(objectKey, recordId);
        this._log('deleteObjectRecord', url);
        return this._request(url, { method: 'delete' });
    }

    deleteObjectRecords(objectKey, recordIds, options) {
        return this._runBatch(recordIds, options, 'deleted', function (recordId) {
            return this.deleteObjectRecord(objectKey, recordId);
        });
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
            try { cache.put(cacheKey, JSON.stringify(data), 60); } catch (e) { this._log('schema cache put failed', String(e)); }
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
            try { cache.put(cacheKey, JSON.stringify(data), 60); } catch (e) { this._log('object schema cache put failed', String(e)); }
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

        const requestedAt = Date.now();
        const resp   = UrlFetchApp.fetch(url, fetchOpts);
        const status = resp.getResponseCode();
        const hdrs   = resp.getAllHeaders() || {};
        const snapshot = this._extractRateLimitSnapshot(hdrs, status);
        this._recordApiUsage({
            method: 'GET',
            url: url,
            attempt: 1,
            ok: status >= 200 && status < 300,
            status: status,
            headers: hdrs,
            requestedAt: requestedAt,
            respondedAt: Date.now()
        });
        this._apiUsage.rateLimit = snapshot;

        if (this.options.debug) {
            this._log('rate-limit', snapshot);
        }

        return {
            remaining: Number.isFinite(snapshot.remaining) ? snapshot.remaining : 0,
            limit: Number.isFinite(snapshot.limit) ? snapshot.limit : 0,
            used: snapshot.used,
            reset: Number.isFinite(snapshot.reset) ? snapshot.reset : 0,
            status: snapshot.status,
            available: snapshot.available,
            reason: snapshot.reason,
            headers: hdrs
        };
    }

    getApiUsageStats() {
        const usage = this._apiUsage || {};
        const rateLimit = usage.rateLimit || this._createEmptyRateLimitSnapshot();
        return {
            run: {
                startedAt: this._formatUsageTimestamp(usage.startedAt),
                totalCalls: usage.totalCalls || 0,
                successfulCalls: usage.successfulCalls || 0,
                failedCalls: usage.failedCalls || 0,
                rateLimitedCalls: usage.rateLimitedCalls || 0,
                byMethod: Object.assign({}, usage.byMethod || {}),
                lastRequestAt: this._formatUsageTimestamp(usage.lastRequestAt),
                lastResponseAt: this._formatUsageTimestamp(usage.lastResponseAt),
                lastRequest: usage.lastRequest ? Object.assign({}, usage.lastRequest, {
                    requestedAt: this._formatUsageTimestamp(usage.lastRequest.requestedAt),
                    respondedAt: this._formatUsageTimestamp(usage.lastRequest.respondedAt)
                }) : null
            },
            daily: Object.assign({}, rateLimit, {
                observedAt: this._formatUsageTimestamp(rateLimit.observedAt),
                headerNames: Array.isArray(rateLimit.headerNames) ? rateLimit.headerNames.slice() : [],
                headers: Object.assign({}, rateLimit.headers || {})
            })
        };
    }

    getRunApiCallCount() {
        return this._apiUsage ? this._apiUsage.totalCalls || 0 : 0;
    }

    getRateLimitStatus() {
        return this.getApiUsageStats().daily;
    }

    resetApiUsageStats(options) {
        const preserveRateLimitSnapshot = !options || options.preserveRateLimitSnapshot !== false;
        this._initApiUsageState(preserveRateLimitSnapshot);
        return this.getApiUsageStats();
    }

    refreshApiUsageStats(objectKey) {
        this.getRateLimit(objectKey);
        return this.getApiUsageStats();
    }

    logApiUsageStats(options) {
        const opts = options || {};
        const stats = this.getApiUsageStats();
        const label = opts.label || 'KnackAPI usage';
        Logger.log('[KnackAPI] ' + label + ' ' + JSON.stringify(stats));
        return stats;
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

        const sorter = Array.isArray(sorters) ? sorters[0] : sorters;
        if (!sorter || !sorter.field) return {};

        const sortOrder = String(sorter.direction || 'asc').toLowerCase() === 'desc' ? 'desc' : 'asc';

        return {
            sort_field: sorter.field,
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

    _getAllPages(fetchPage, options, label) {
        const o = options || {};
        const rows = o.rows || 1000;
        const pageConcurrency = Number.isFinite(o.pageConcurrency)
            ? Math.max(1, Math.floor(o.pageConcurrency))
            : this.options.pageConcurrency;

        const firstPage = fetchPage.call(this, Object.assign({}, o, {
            page: 1,
            rows: rows,
            rawResponse: true
        }));
        const totalPages = firstPage.total_pages || 1;
        const totalRecords = firstPage.total_records || 0;
        let all = firstPage.records && firstPage.records.length ? firstPage.records.slice() : [];

        this._log(label + ': paging', { totalPages: totalPages, total_records: totalRecords });

        if (totalPages <= 1) return all;

        for (let start = 2; start <= totalPages; start += pageConcurrency) {
            const end = Math.min(totalPages, start + pageConcurrency - 1);
            const pages = [];
            for (let page = start; page <= end; page++) pages.push(page);

            const responses = pageConcurrency > 1
                ? this._fetchPageBatch(fetchPage, o, rows, pages)
                : pages.map(function (page) {
                    return fetchPage.call(this, Object.assign({}, o, { page: page, rows: rows, rawResponse: true }));
                }, this);

            responses.forEach(function (resp) {
                if (resp && resp.records && resp.records.length) all = all.concat(resp.records);
            });

            if (typeof o.onProgress === 'function') {
                o.onProgress({
                    page: end,
                    totalPages: totalPages,
                    recordsLoaded: all.length,
                    totalRecords: totalRecords,
                    percentage: Math.round((end / totalPages) * 100)
                });
            }
        }

        return all;
    }

    _fetchPageBatch(fetchPage, baseOptions, rows, pages) {
        const urls = pages.map(function (page) {
            const response = fetchPage.call(this, Object.assign({}, baseOptions, { page: page, rows: rows, rawResponse: true, __urlOnly: true }));
            return response.url;
        }, this);
        const requestOptions = urls.map(function (url) {
            return {
                url: url,
                method: 'get',
                headers: this._buildHeaders(),
                muteHttpExceptions: true,
                followRedirects: true,
                validateHttpsCertificates: true
            };
        }, this);

        const responses = UrlFetchApp.fetchAll(requestOptions);
        return responses.map(function (resp, index) {
            const url = urls[index];
            const code = resp.getResponseCode();
            const text = resp.getContentText();
            this._recordApiUsage({
                method: 'GET',
                url: url,
                attempt: 1,
                ok: code >= 200 && code < 300,
                status: code,
                headers: resp.getAllHeaders(),
                requestedAt: Date.now(),
                respondedAt: Date.now()
            });

            if (code >= 200 && code < 300) return this._safeJson(text);

            if (this.options.retryOnStatus.indexOf(code) !== -1) {
                return this._request(url, { method: 'get' });
            }

            throw this._makeError('KnackAPI batch page request failed', code, url, text);
        }, this);
    }

    _normalizeUpdateBatch(recordIds, recordData, options) {
        const records = Array.isArray(recordIds) ? recordIds : [];
        const isPerRecord = records.length > 0 && records.every(function (record) {
            return record && typeof record === 'object' && (('id' in record && 'data' in record) || ('recordId' in record && 'recordData' in record));
        });

        if (isPerRecord) {
            return {
                records: records.map(function (record) {
                    return 'id' in record ? { id: record.id, data: record.data } : { id: record.recordId, data: record.recordData };
                }).filter(function (record) { return record.id; }),
                options: recordData && typeof recordData === 'object' && !Array.isArray(recordData) ? recordData : {}
            };
        }

        return {
            records: records.filter(Boolean).map(function (recordId) { return { id: recordId, data: recordData }; }),
            options: options || {}
        };
    }

    _runBatch(items, options, successKey, execute) {
        const o = options || {};
        const list = Array.isArray(items) ? items.filter(function (item) { return item; }) : [];
        const total = list.length;
        const results = [];
        const failedItems = [];
        const continueOnError = o.continueOnError === true;
        let success = 0;
        let failed = 0;

        for (let index = 0; index < total; index++) {
            try {
                const result = execute.call(this, list[index], index);
                success++;
                results[index] = result;
                if (typeof o.onProgress === 'function') {
                    o.onProgress({ total: total, index: index, success: success, failed: failed, result: result });
                }
            } catch (error) {
                failed++;
                failedItems.push({ index: index, item: list[index], error: error });
                if (typeof o.onProgress === 'function') {
                    o.onProgress({ total: total, index: index, success: success, failed: failed, error: error });
                }
                if (!continueOnError) throw error;
            }

            if (o.staggerMs && index < total - 1) {
                Utilities.sleep(Math.max(0, Math.floor(o.staggerMs)));
            }
        }

        const summary = { total: total, failed: failed, records: results.filter(function (result) { return !!result; }), failures: failedItems };
        summary[successKey] = success;
        return summary;
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

        if (options.__urlOnly) return { url: url };

        const fetchOpts = {
            method: method,
            headers: this._buildHeaders(),
            muteHttpExceptions: true,
            followRedirects: true,
            validateHttpsCertificates: true
        };
        if (options.payload != null) fetchOpts.payload = options.payload;
        if (options.contentType) fetchOpts.contentType = options.contentType;

        const maxRetries = this.options.maxRetries;
        let attempt = 0;

        for (;;) {
            const requestedAt = Date.now();
            try {
                const resp = UrlFetchApp.fetch(url, fetchOpts);
                const code = resp.getResponseCode();
                const text = resp.getContentText();
                const headers = resp.getAllHeaders();

                this._recordApiUsage({
                    method: method,
                    url: url,
                    attempt: attempt + 1,
                    ok: code >= 200 && code < 300,
                    status: code,
                    headers: headers,
                    requestedAt: requestedAt,
                    respondedAt: Date.now()
                });

                if (code >= 200 && code < 300) {
                    const json = this._safeJson(text);
                    if (this.options.debug) this._log('response(ok)', { code: code, bytes: text ? text.length : 0 });
                    return json;
                }

                if (this.options.retryOnStatus.indexOf(code) !== -1) {
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
                if (e && e.status !== undefined) throw e;
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
            const ra = headers && (headers['Retry-After'] || headers['retry-after']);
            if (!ra) return 0;
            const num = Number(ra);
            if (!isNaN(num)) return Math.max(0, num * 1000);
        } catch (e) { this._log('retry-after parse failed', String(e)); }
        return 0;
    }

    _backoffMs(attempt) {
        const base = this.options.retryDelayBase * Math.pow(2, Math.max(0, attempt - 1));
        const wait = Math.min(this.options.retryDelayMax, base);
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

    _initApiUsageState(preserveRateLimitSnapshot) {
        const previousRateLimit = preserveRateLimitSnapshot && this._apiUsage && this._apiUsage.rateLimit
            ? this._apiUsage.rateLimit
            : this._createEmptyRateLimitSnapshot();
        this._apiUsage = {
            startedAt: Date.now(),
            totalCalls: 0,
            successfulCalls: 0,
            failedCalls: 0,
            rateLimitedCalls: 0,
            byMethod: {},
            lastRequestAt: null,
            lastResponseAt: null,
            lastRequest: null,
            rateLimit: previousRateLimit
        };
    }

    _createEmptyRateLimitSnapshot() {
        return {
            available: false,
            reason: 'No rate-limit headers observed yet.',
            limit: null,
            remaining: null,
            used: null,
            reset: null,
            status: null,
            observedAt: null,
            headerNames: [],
            headers: {}
        };
    }

    _formatUsageTimestamp(timestampMs) {
        return Number.isFinite(timestampMs) ? new Date(timestampMs).toISOString() : null;
    }

    _normalizeHeaders(headers) {
        const normalized = {};
        Object.keys(headers || {}).forEach(function (key) {
            normalized[String(key).toLowerCase()] = headers[key];
        });
        return normalized;
    }

    _parseHeaderInt(value) {
        if (value === undefined || value === null || value === '') return null;
        const parsed = parseInt(String(value), 10);
        return Number.isFinite(parsed) ? parsed : null;
    }

    _extractRateLimitSnapshot(headersLike, status) {
        const headers = this._normalizeHeaders(headersLike);
        const headerNames = Object.keys(headers).sort();
        const remaining = this._parseHeaderInt(headers['x-planlimit-remaining'] || headers['x-rate-limit-remaining']);
        const limit = this._parseHeaderInt(headers['x-planlimit-limit'] || headers['x-rate-limit-limit']);
        const reset = this._parseHeaderInt(headers['x-planlimit-reset'] || headers['x-rate-limit-reset']);
        const available = remaining !== null || limit !== null || reset !== null;

        return {
            available: available,
            reason: available ? null : 'Knack did not return readable rate-limit headers for this request.',
            limit: limit,
            remaining: remaining,
            used: Number.isFinite(limit) && Number.isFinite(remaining) ? Math.max(0, limit - remaining) : null,
            reset: reset,
            status: Number.isFinite(status) ? status : null,
            observedAt: Date.now(),
            headerNames: headerNames,
            headers: headers
        };
    }

    _recordApiUsage(details) {
        if (!this._apiUsage) this._initApiUsageState();
        const method = String(details.method || 'GET').toUpperCase();
        const status = Number.isFinite(details.status) ? details.status : null;
        const ok = details.ok === true;
        const usage = this._apiUsage;

        usage.totalCalls++;
        usage.byMethod[method] = (usage.byMethod[method] || 0) + 1;
        usage.lastRequestAt = details.requestedAt || Date.now();
        usage.lastResponseAt = details.respondedAt || Date.now();
        if (ok) usage.successfulCalls++;
        else usage.failedCalls++;
        if (status === 429) usage.rateLimitedCalls++;
        usage.lastRequest = {
            method: method,
            url: details.url || '',
            attempt: Number.isFinite(details.attempt) ? details.attempt : 1,
            status: status,
            ok: ok,
            requestedAt: usage.lastRequestAt,
            respondedAt: usage.lastResponseAt
        };
        usage.rateLimit = this._extractRateLimitSnapshot(details.headers, status);
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
            } catch (e) {
                payload = String(data);
            }
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