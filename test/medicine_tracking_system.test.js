"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const app = require("../medicine_tracking_system.js");

test("valid medicine entries preserve category, unit, and minimum quantity", () => {
  assert.deepEqual(app.buildMedicine({
    name: "  Amoxicillin ",
    category: "Antibiotic",
    unit: "capsules",
    minLevel: 20,
  }), {
    name: "Amoxicillin",
    category: "Antibiotic",
    unit: "capsules",
    minLevel: 20,
  });
});

test("medicine validation rejects unsupported categories and negative minimums", () => {
  assert.throws(() => app.buildMedicine({
    name: "Medicine",
    category: "Unknown",
    unit: "tablets",
    minLevel: 2,
  }), /Category must be one of/);
  assert.throws(() => app.buildMedicine({
    name: "Medicine",
    category: "Antibiotic",
    unit: "tablets",
    minLevel: -1,
  }), /Minimum quantity must be an integer/);
});

test("health-centre validation stores a GeoJSON point", () => {
  const centre = app.buildHealthCentre({
    name: "West Clinic",
    district: "Luwero",
    level: "HC III",
    longitude: 32.5,
    latitude: 0.8,
  });
  assert.deepEqual(centre.location, {
    type: "Point",
    coordinates: [32.5, 0.8],
  });
});

test("health-centre validation rejects out-of-range coordinates", () => {
  assert.throws(() => app.buildHealthCentre({
    name: "West Clinic",
    district: "Luwero",
    level: "HC III",
    longitude: 181,
    latitude: 0,
  }), /Longitude must be between/);
});

test("supplier medicine references are normalized and deduplicated", () => {
  assert.deepEqual(app.buildSupplier({
    name: "  NMS ",
    contact: "0700000000",
    supplies: [101, "102", 101],
  }), {
    name: "NMS",
    contact: "0700000000",
    supplies: [101, 102],
  });
});

test("stock decorations identify low, expired, and soon-to-expire records", () => {
  const base = Date.now();
  const records = app.decorateStock([
    { medicine: "Low", quantity: 1, minLevel: 2, expiry: new Date(base + 200 * 86400000) },
    { medicine: "Expired", quantity: 3, minLevel: 2, expiry: new Date(base - 86400000) },
    { medicine: "Expiring", quantity: 3, minLevel: 2, expiry: new Date(base + 5 * 86400000) },
    { medicine: "Healthy", quantity: 3, minLevel: 2, expiry: new Date(base + 200 * 86400000) },
  ]);
  assert.deepEqual(records.map((record) => record.flags), [
    ["LOW"],
    ["EXPIRED"],
    ["EXPIRING"],
    [],
  ]);
});

test("sample data retains all original medicine, centre, supplier, and stock records", () => {
  const samples = app.sampleData();
  assert.equal(samples.healthcentres.length, 5);
  assert.equal(samples.medicines.length, 5);
  assert.equal(samples.suppliers.length, 5);
  assert.equal(samples.stockrecords.length, 8);
});

test("browser interface includes every medicine in its scrollable inventory chart", () => {
  const match = app.APP_HTML.match(/<script>([\s\S]*?)<\/script>/);
  assert.ok(match, "browser script should be present");
  assert.doesNotThrow(() => new Function(match[1]));
  assert.match(match[1], /state\.dashboard\.stock/);
  assert.match(match[1], /inventory graph/i);
});
