// Smoke test for the ingest API.
// Usage: k6 run load/ingest-smoke.js
//
// Ramps to 200 VUs holding for 1 minute against POST /api/v1/events.
// The service is expected to sustain this comfortably — most of the work is
// Avro serialization + async Kafka producer send.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
    thresholds: {
        http_req_failed:   ['rate<0.01'],
        http_req_duration: ['p(95)<150'],
    },
    scenarios: {
        firehose: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '30s', target: 50  },
                { duration: '1m',  target: 200 },
                { duration: '15s', target: 0   },
            ],
            gracefulRampDown: '5s',
        },
    },
};

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const TYPES = ['page_view', 'add_to_cart', 'checkout_completed', 'search'];

export default function () {
    const body = JSON.stringify({
        type:   TYPES[randomIntBetween(0, TYPES.length - 1)],
        userId: `user-${randomIntBetween(1, 100000)}`,
        payload: {
            path:     '/home',
            referrer: 'google',
        },
    });

    const res = http.post(`${BASE}/api/v1/events`, body, {
        headers: { 'Content-Type': 'application/json' },
    });

    check(res, {
        '202 Accepted': (r) => r.status === 202,
        'has eventId':  (r) => r.json('eventId') !== undefined,
    });
    sleep(0.1);
}
