import http from "k6/http";
import { sleep, check } from "k6";

// Base URL of your Laravel app
// const BASE_URL = "http://localhost:8000/api"; // Replace with your app's URL
const BASE_URL = "http://192.168.49.2:30007/api";

export const options = {
  stages: [
    { duration: "2m", target: 50 }, // Ramp up to 1,000 users over 2 minutes
    { duration: "5m", target: 100 }, // Ramp up to 10,000 users over 5 minutes
    { duration: "5m", target: 100 }, // Hold at 10,000 users for 5 minutes
    { duration: "2m", target: 0 }, // Ramp down to 0 users over 2 minutes
  ],
  thresholds: {
    http_req_duration: ["p(95)<500"], // 95% of requests should complete in under 500ms
    http_req_failed: ["rate<0.01"], // Error rate should be less than 1%
  },
};

export default function () {
  // Simulate a mix of read and write operations
  const rand = Math.random();

  if (rand < 0.6) {
    // 60% chance: Read all people (GET /api/people)
    let res = http.get(`${BASE_URL}/people`);
    check(res, {
      "GET /api/people status is 200": (r) => r.status === 200,
    });
  } else if (rand < 0.8) {
    // 20% chance: Read a specific person (GET /api/people/{id})
    const id = Math.floor(Math.random() * 1000) + 1; // Random ID from 1 to 1000
    let res = http.get(`${BASE_URL}/people/${id}`);
    check(res, {
      "GET /api/people/{id} status is 200": (r) => r.status === 200,
    });
  } else if (rand < 0.9) {
    // 10% chance: Create a person (POST /api/people)
    let payload = JSON.stringify({
      name: `TestUser${Math.floor(Math.random() * 10000)}`,
      age: Math.floor(Math.random() * 63) + 18, // Ages 18-80
    });
    let res = http.post(`${BASE_URL}/people`, payload, {
      headers: { "Content-Type": "application/json" },
    });
    check(res, {
      "POST /api/people status is 200": (r) => r.status === 200,
    });
  } else {
    // 10% chance: Update or delete a random person
    const id = Math.floor(Math.random() * 1000) + 1;
    if (Math.random() < 0.5) {
      // Update (PUT /api/people/{id})
      let payload = JSON.stringify({
        name: `UpdatedUser${id}`,
        age: Math.floor(Math.random() * 63) + 18,
      });
      let res = http.put(`${BASE_URL}/people/${id}`, payload, {
        headers: { "Content-Type": "application/json" },
      });
      check(res, {
        "PUT /api/people/{id} status is 200": (r) => r.status === 200,
      });
    } else {
      // Delete (DELETE /api/people/{id})
      let res = http.del(`${BASE_URL}/people/${id}`);
      check(res, {
        "DELETE /api/people/{id} status is 204": (r) => r.status === 204,
      });
    }
  }

  // Simulate user think time (pause between requests)
  sleep(1); // 1 second pause
}
