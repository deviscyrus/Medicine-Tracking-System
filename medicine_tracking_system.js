"use strict";

/**
 * Medicine Tracking System
 * ------------------------
 * Local Node.js web application for managing medicine catalogue, centres,
 * suppliers, stock, movement history, reports, and MongoDB ACID workflows.
 *
 * Start with `npm install` followed by `npm start`. The interface is served at
 * http://127.0.0.1:3000 by default. Set MONGO_URI and PORT to customize it.
 */

const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const { MongoClient, ReadConcern, WriteConcern } = require("mongodb");

/** Load the project's local settings without replacing environment overrides. */
function loadLocalConfig() {
  const file = path.join(__dirname, "med_connect.env");
  if (!fs.existsSync(file)) return;
  for (const line of fs.readFileSync(file, "utf8").split(/\r?\n/)) {
    const match = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/);
    if (!match || Object.hasOwn(process.env, match[1])) continue;
    process.env[match[1]] = match[2].replace(/^(['"])(.*)\1$/, "$2");
  }
}

loadLocalConfig();

const MONGO_URI = process.env.MONGO_URI || process.env.MONGODB_URI || "mongodb://127.0.0.1:27017";
const DB_NAME = process.env.MONGO_DATABASE || process.env.MONGO_DB_NAME || "medicine_tracking_system_db";
const PORT = Number.parseInt(process.env.PORT || "3000", 10);
const HOST = "127.0.0.1";
const BODY_LIMIT = 1024 * 1024;
const LEVELS = ["HC II", "HC III", "HC IV", "Hospital"];
const CATEGORIES = ["Antimalarial", "Antibiotic", "Painkiller", "Rehydration"];
const UNITS = ["tablets", "capsules", "sachets"];
const ID_BASE = { healthcentres: 0, medicines: 100, suppliers: 200 };
const COLLECTION_SCHEMAS = {
  healthcentres: {
    bsonType: "object",
    required: ["name", "district", "level", "location"],
    properties: {
      name: { bsonType: "string", minLength: 2 },
      district: { bsonType: "string", minLength: 2 },
      level: { enum: LEVELS },
      location: {
        bsonType: "object",
        required: ["type", "coordinates"],
        properties: {
          type: { enum: ["Point"] },
          coordinates: { bsonType: "array", minItems: 2, maxItems: 2 },
        },
      },
    },
  },
  medicines: {
    bsonType: "object",
    required: ["name", "category", "unit", "minLevel"],
    properties: {
      name: { bsonType: "string", minLength: 2 },
      category: { enum: CATEGORIES },
      unit: { enum: UNITS },
      minLevel: { bsonType: ["int", "long"], minimum: 0 },
    },
  },
  suppliers: {
    bsonType: "object",
    required: ["name", "contact", "supplies"],
    properties: {
      name: { bsonType: "string", minLength: 2 },
      contact: { bsonType: "string" },
      supplies: { bsonType: "array", items: { bsonType: ["int", "long"] } },
    },
  },
  stockrecords: {
    bsonType: "object",
    required: ["centreId", "medicineId", "quantity", "expiry", "recorded"],
    properties: {
      centreId: { bsonType: ["int", "long"] },
      medicineId: { bsonType: ["int", "long"] },
      quantity: { bsonType: ["int", "long"], minimum: 0 },
      expiry: { bsonType: "date" },
      recorded: { bsonType: "date" },
    },
  },
  stock_movements: {
    bsonType: "object",
    required: ["type", "medicineId", "quantity", "at"],
    properties: {
      type: { enum: ["DISPENSE", "RECEIVE", "TRANSFER", "ADJUST"] },
      medicineId: { bsonType: ["int", "long"] },
      quantity: { bsonType: ["int", "long"], minimum: 0 },
      at: { bsonType: "date" },
    },
  },
};

class BusinessError extends Error {
  /** Create an error for an invalid business operation or missing record. */
  constructor(message) {
    super(message);
    this.name = "BusinessError";
    this.statusCode = 400;
  }
}

let client;
let db;
let transactionsSupported = false;

/** Return the current time as a JavaScript Date for MongoDB UTC timestamps. */
function now() {
  return new Date();
}

/** Return a valid integer or raise a clear validation error. */
function integer(value, label, { minimum = Number.MIN_SAFE_INTEGER } = {}) {
  if (typeof value === "string" && value.trim() === "") {
    throw new BusinessError(`${label} is required.`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < minimum) {
    throw new BusinessError(`${label} must be an integer greater than or equal to ${minimum}.`);
  }
  return parsed;
}

/** Validate required text values and trim surrounding whitespace. */
function text(value, label, minimumLength = 1) {
  if (typeof value !== "string" || value.trim().length < minimumLength) {
    throw new BusinessError(`${label} must contain at least ${minimumLength} character(s).`);
  }
  return value.trim();
}

/** Validate and normalize a submitted date, interpreting date-only values as UTC. */
function dateValue(value, label = "Date") {
  const dateOnly = typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value);
  const candidate = dateOnly ? new Date(`${value}T00:00:00.000Z`) : new Date(value);
  if (Number.isNaN(candidate.getTime())) {
    throw new BusinessError(`${label} must be a valid date.`);
  }
  if (dateOnly && candidate.toISOString().slice(0, 10) !== value) {
    throw new BusinessError(`${label} must be a valid calendar date.`);
  }
  return candidate;
}

/** Return a valid finite floating-point number for geographic coordinates. */
function coordinate(value, label, minimum, maximum) {
  if (typeof value === "string" && value.trim() === "") {
    throw new BusinessError(`${label} is required.`);
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < minimum || parsed > maximum) {
    throw new BusinessError(`${label} must be between ${minimum} and ${maximum}.`);
  }
  return parsed;
}

/** Ensure a choice value is included in its supported option list. */
function choice(value, options, label) {
  if (!options.includes(value)) {
    throw new BusinessError(`${label} must be one of: ${options.join(", ")}.`);
  }
  return value;
}

/** Add session options only when a transaction session is active. */
function sessionOptions(session) {
  return session ? { session } : {};
}

/** Connect to MongoDB, prepare validated collections, and detect transaction support. */
async function connectDatabase() {
  client = new MongoClient(MONGO_URI, {
    serverSelectionTimeoutMS: 5000,
    writeConcern: { w: "majority", j: true },
  });
  await client.connect();
  const hello = await client.db("admin").command({ hello: 1 });
  transactionsSupported = Boolean(hello.setName) || hello.msg === "isdbgrid";
  db = client.db(DB_NAME);

  await setupDatabase();
}

/** Create collection validators and indexes while retaining existing documents. */
async function setupDatabase() {
  const existing = new Set(await db.listCollections({}, { nameOnly: true }).toArray().then(
    (collections) => collections.map((collection) => collection.name),
  ));
  for (const [name, schema] of Object.entries(COLLECTION_SCHEMAS)) {
    if (!existing.has(name)) {
      await db.createCollection(name, { validator: { $jsonSchema: schema } });
    }
  }
  await Promise.all([
    db.collection("healthcentres").createIndex({ name: 1 }, { unique: true }),
    db.collection("healthcentres").createIndex({ location: "2dsphere" }),
    db.collection("medicines").createIndex({ name: 1 }, { unique: true }),
    db.collection("suppliers").createIndex({ name: 1 }, { unique: true }),
    db.collection("stockrecords").createIndex(
      { centreId: 1, medicineId: 1 },
      { unique: true },
    ),
    db.collection("stockrecords").createIndex({ expiry: 1 }),
    db.collection("stock_movements").createIndex({ at: -1 }),
  ]);
}

/** Execute multiple writes atomically when the server supports transactions. */
async function runAtomic(work) {
  if (!transactionsSupported) {
    return work(null);
  }
  const session = client.startSession();
  try {
    return await session.withTransaction(
      () => work(session),
      {
        readConcern: new ReadConcern("snapshot"),
        writeConcern: new WriteConcern("majority"),
      },
    );
  } finally {
    await session.endSession();
  }
}

/** Generate the next human-readable identifier for an entity collection. */
async function nextId(collectionName, session) {
  const counter = await db.collection("counters").findOneAndUpdate(
    { _id: collectionName },
    { $inc: { seq: 1 } },
    { upsert: true, returnDocument: "after", ...sessionOptions(session) },
  );
  return ID_BASE[collectionName] + counter.seq;
}

/** Check that a referenced document exists before a write. */
async function requireDocument(collectionName, id, label, session) {
  const document = await db.collection(collectionName).findOne(
    { _id: id },
    { projection: { _id: 1 }, ...sessionOptions(session) },
  );
  if (!document) {
    throw new BusinessError(`${label} with name/ID ${id} does not exist.`);
  }
}

/** Validate submitted fields and build the stored health-centre document. */
function buildHealthCentre(input) {
  return {
    name: text(input.name, "Name", 2),
    district: text(input.district, "District", 2),
    level: choice(input.level, LEVELS, "Health-centre level"),
    location: {
      type: "Point",
      coordinates: [
        coordinate(input.longitude, "Longitude", -180, 180),
        coordinate(input.latitude, "Latitude", -90, 90),
      ],
    },
  };
}

/** Validate medicine details, including its type and reorder threshold. */
function buildMedicine(input) {
  return {
    name: text(input.name, "Name", 2),
    category: choice(input.category, CATEGORIES, "Category"),
    unit: choice(input.unit, UNITS, "Medicine type"),
    minLevel: integer(input.minLevel, "Minimum quantity", { minimum: 0 }),
  };
}

/** Validate supplier details and normalize the medicines it can supply. */
function buildSupplier(input) {
  if (!Array.isArray(input.supplies)) {
    throw new BusinessError("Choose one or more medicines supplied.");
  }
  return {
    name: text(input.name, "Name", 2),
    contact: text(input.contact, "Contact"),
    supplies: [...new Set(input.supplies.map((id) => integer(id, "Medicine ID", { minimum: 0 })))],
  };
}

/** Return an aggregation that joins stock to medicine and health-centre names. */
function stockPipeline(match = null) {
  const pipeline = [
    {
      $lookup: {
        from: "healthcentres",
        localField: "centreId",
        foreignField: "_id",
        as: "centreDocument",
      },
    },
    {
      $lookup: {
        from: "medicines",
        localField: "medicineId",
        foreignField: "_id",
        as: "medicineDocument",
      },
    },
    { $unwind: "$centreDocument" },
    { $unwind: "$medicineDocument" },
    {
      $project: {
        _id: 0,
        centreId: 1,
        centre: "$centreDocument.name",
        medicineId: 1,
        medicine: "$medicineDocument.name",
        category: "$medicineDocument.category",
        unit: "$medicineDocument.unit",
        quantity: 1,
        minLevel: "$medicineDocument.minLevel",
        expiry: 1,
      },
    },
  ];
  if (match) pipeline.push({ $match: match });
  pipeline.push({ $sort: { centreId: 1, medicineId: 1 } });
  return pipeline;
}

/** Return stock rows decorated with low-stock, expired, and expiry-window flags. */
function decorateStock(records) {
  const current = now();
  const cutoff = current.getTime() + 90 * 24 * 60 * 60 * 1000;
  return records.map((record) => {
    const expiresAt = new Date(record.expiry).getTime();
    const flags = [];
    if (record.quantity < record.minLevel) flags.push("LOW");
    if (expiresAt < current.getTime()) flags.push("EXPIRED");
    else if (expiresAt <= cutoff) flags.push("EXPIRING");
    return { ...record, expiry: new Date(record.expiry), flags };
  });
}

/** Write a stock movement audit record within the caller's transaction. */
async function logMovement(session, type, medicineId, quantity, extra = {}) {
  await db.collection("stock_movements").insertOne(
    { type, medicineId, quantity, at: now(), ...extra },
    sessionOptions(session),
  );
}

/** Decrement stock only when enough units are available. */
async function takeStock(session, centreId, medicineId, quantity) {
  const result = await db.collection("stockrecords").updateOne(
    { centreId, medicineId, quantity: { $gte: quantity } },
    { $inc: { quantity: -quantity }, $set: { recorded: now() } },
    sessionOptions(session),
  );
  if (!result.matchedCount) {
    throw new BusinessError(`Not enough stock of medicine ${medicineId} at centre ${centreId}.`);
  }
}

/** Add stock to a location, creating its record if it has no prior stock. */
async function putStock(session, centreId, medicineId, quantity, expiry) {
  await db.collection("stockrecords").updateOne(
    { centreId, medicineId },
    {
      $inc: { quantity },
      $set: { recorded: now() },
      $min: { expiry },
      $setOnInsert: { centreId, medicineId },
    },
    { upsert: true, ...sessionOptions(session) },
  );
}

/** Build the sample records used by the optional demo-data action. */
function sampleData() {
  const point = (longitude, latitude) => ({
    type: "Point",
    coordinates: [longitude, latitude],
  });
  const day = (value) => new Date(`${value}T00:00:00.000Z`);
  return {
    healthcentres: [
      { _id: 1, name: "Kasana HC III", district: "Luwero", level: "HC III", location: point(32.50, 0.84) },
      { _id: 2, name: "Bombo HC IV", district: "Luwero", level: "HC IV", location: point(32.53, 0.58) },
      { _id: 3, name: "Wobulenzi HC IV", district: "Luwero", level: "HC IV", location: point(32.48, 0.72) },
      { _id: 4, name: "Kiwoko HC III", district: "Nakaseke", level: "HC III", location: point(32.40, 0.89) },
      { _id: 5, name: "Zirobwe HC II", district: "Luwero", level: "HC II", location: point(32.60, 0.67) },
    ],
    medicines: [
      { _id: 101, name: "Coartem", category: "Antimalarial", unit: "tablets", minLevel: 200 },
      { _id: 102, name: "Amoxicillin", category: "Antibiotic", unit: "capsules", minLevel: 300 },
      { _id: 103, name: "Paracetamol", category: "Painkiller", unit: "tablets", minLevel: 500 },
      { _id: 104, name: "ORS Sachets", category: "Rehydration", unit: "sachets", minLevel: 150 },
      { _id: 105, name: "Cotrimoxazole", category: "Antibiotic", unit: "tablets", minLevel: 250 },
    ],
    suppliers: [
      { _id: 201, name: "National Medical Stores", contact: "+256700000001", supplies: [101, 102, 103, 104, 105] },
      { _id: 202, name: "Joint Medical Store", contact: "+256700000002", supplies: [101, 102, 105] },
      { _id: 203, name: "Kampala Pharma Ltd", contact: "+256700000003", supplies: [102, 103] },
      { _id: 204, name: "Rehydrate Uganda", contact: "+256700000004", supplies: [104] },
      { _id: 205, name: "Mulago Wholesale Pharmacy", contact: "+256700000005", supplies: [103, 105] },
    ],
    stockrecords: [
      { centreId: 1, medicineId: 101, quantity: 80, expiry: day("2027-03-01") },
      { centreId: 1, medicineId: 102, quantity: 450, expiry: day("2027-06-15") },
      { centreId: 2, medicineId: 101, quantity: 600, expiry: day("2027-08-20") },
      { centreId: 2, medicineId: 103, quantity: 90, expiry: day("2026-12-10") },
      { centreId: 3, medicineId: 104, quantity: 300, expiry: day("2027-01-30") },
      { centreId: 4, medicineId: 105, quantity: 60, expiry: day("2026-11-25") },
      { centreId: 5, medicineId: 101, quantity: 40, expiry: day("2027-02-14") },
      { centreId: 5, medicineId: 102, quantity: 700, expiry: day("2027-09-09") },
    ],
  };
}

/** Insert demo documents only into collections that are currently empty. */
async function seedDatabase() {
  const data = sampleData();
  for (const [name, documents] of Object.entries(data)) {
    const collection = db.collection(name);
    if (await collection.countDocuments({}) === 0) {
      const withRecordedAt = name === "stockrecords"
        ? documents.map((document) => ({ ...document, recorded: now() }))
        : documents;
      await collection.insertMany(withRecordedAt);
    }
  }
  for (const [name, sequence] of Object.entries({
    healthcentres: 5,
    medicines: 5,
    suppliers: 5,
  })) {
    await db.collection("counters").updateOne(
      { _id: name },
      { $max: { seq: sequence } },
      { upsert: true },
    );
  }
}

/** Read a bounded JSON request body and reject malformed or oversized input. */
async function readJson(request) {
  let size = 0;
  const chunks = [];
  for await (const chunk of request) {
    size += chunk.length;
    if (size > BODY_LIMIT) throw new BusinessError("Request body exceeds the 1 MB limit.");
    chunks.push(chunk);
  }
  if (!chunks.length) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new BusinessError("Request body must contain valid JSON.");
  }
}

/** Return the JSON response and consistent no-cache headers. */
function sendJson(response, status, payload) {
  const body = JSON.stringify(payload);
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body),
    "cache-control": "no-store",
    "x-content-type-options": "nosniff",
  });
  response.end(body);
}

/** Return the static browser application. */
function sendApp(response) {
  response.writeHead(200, {
    "content-type": "text/html; charset=utf-8",
    "cache-control": "no-store",
    "x-content-type-options": "nosniff",
    "content-security-policy": "default-src 'self' 'unsafe-inline' blob:; connect-src 'self';",
  });
  response.end(APP_HTML);
}

/** Convert MongoDB duplicate-key errors into a client-safe conflict response. */
function normalizeError(error) {
  if (error?.code === 11000) {
    return { status: 409, message: "A record with that name or centre/medicine pair already exists." };
  }
  if (error instanceof BusinessError) {
    return { status: error.statusCode, message: error.message };
  }
  if (error?.name === "MongoServerError" && error.code === 121) {
    return { status: 400, message: "The submitted record does not satisfy the database validation rules." };
  }
  return { status: 500, message: "The request could not be completed. Check the server console for details." };
}

/** Filter an entity list with an escaped, case-insensitive name search. */
function escapedRegex(value) {
  return new RegExp(value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
}

/** List the supported entity collection names used by generic CRUD routes. */
function entityCollection(name) {
  if (!Object.hasOwn(ID_BASE, name)) {
    throw new BusinessError("Unknown record type.");
  }
  return db.collection(name);
}

/** Require a medicines or centres document and return it for subsequent checks. */
async function findRequired(collectionName, id, label, session) {
  const document = await db.collection(collectionName).findOne(
    { _id: id },
    sessionOptions(session),
  );
  if (!document) throw new BusinessError(`${label} not found.`);
  return document;
}

/** Return the current stock, joined entity lists, and dashboard data. */
async function dashboardData() {
  const [centres, medicines, suppliers, rawStock, movements] = await Promise.all([
    db.collection("healthcentres").find({}).sort({ _id: 1 }).toArray(),
    db.collection("medicines").find({}).sort({ _id: 1 }).toArray(),
    db.collection("suppliers").find({}).sort({ _id: 1 }).toArray(),
    db.collection("stockrecords").aggregate(stockPipeline()).toArray(),
    db.collection("stock_movements").find({}).sort({ at: -1 }).limit(10).toArray(),
  ]);
  const stock = decorateStock(rawStock);
  const current = now();
  const low = stock.filter((record) => record.flags.includes("LOW"));
  const expired = stock.filter((record) => record.flags.includes("EXPIRED"));
  const expiring = stock.filter((record) => record.flags.includes("EXPIRING"));
  const quantities = new Map();
  for (const record of rawStock) {
    quantities.set(record.medicineId, (quantities.get(record.medicineId) || 0) + record.quantity);
  }
  const chart = medicines.map((medicine) => ({
    medicineId: medicine._id,
    name: medicine.name,
    unit: medicine.unit,
    quantity: quantities.get(medicine._id) || 0,
  }));
  return {
    centres,
    medicines,
    suppliers,
    stock,
    movements,
    transactionsSupported,
    totals: {
      centres: centres.length,
      medicines: medicines.length,
      suppliers: suppliers.length,
      stockRecords: stock.length,
      units: rawStock.reduce((sum, record) => sum + record.quantity, 0),
      low: low.length,
      expired: expired.length,
      expiring: expiring.length,
      alerts: low.length + expired.length + expiring.length,
    },
    chart,
    alerts: [...low, ...expired, ...expiring].slice(0, 30),
  };
}

/** Create a stock movement for a dispense and reduce on-hand inventory. */
async function dispenseMedicine(input) {
  const centreId = integer(input.centreId, "Health centre", { minimum: 0 });
  const medicineId = integer(input.medicineId, "Medicine", { minimum: 0 });
  const quantity = integer(input.quantity, "Quantity", { minimum: 1 });
  const patientRef = text(input.patientRef, "Patient reference");
  await runAtomic(async (session) => {
    await requireDocument("healthcentres", centreId, "Health centre", session);
    await requireDocument("medicines", medicineId, "Medicine", session);
    await takeStock(session, centreId, medicineId, quantity);
    await logMovement(session, "DISPENSE", medicineId, quantity, { centreId, patientRef });
  });
}

/** Receive supplier stock, update inventory, and store its audit record. */
async function receiveDelivery(input) {
  const supplierId = integer(input.supplierId, "Supplier", { minimum: 0 });
  const centreId = integer(input.centreId, "Health centre", { minimum: 0 });
  const medicineId = integer(input.medicineId, "Medicine", { minimum: 0 });
  const quantity = integer(input.quantity, "Quantity", { minimum: 1 });
  const expiry = dateValue(input.expiry, "Expiry date");
  await runAtomic(async (session) => {
    const supplier = await findRequired("suppliers", supplierId, "Supplier", session);
    if (!supplier.supplies.includes(medicineId)) {
      throw new BusinessError(`${supplier.name} does not supply the selected medicine.`);
    }
    await requireDocument("healthcentres", centreId, "Health centre", session);
    await requireDocument("medicines", medicineId, "Medicine", session);
    await putStock(session, centreId, medicineId, quantity, expiry);
    await logMovement(session, "RECEIVE", medicineId, quantity, { centreId, supplierId });
  });
}

/** Transfer stock between distinct centres without leaving a partial update. */
async function transferStock(input) {
  const sourceId = integer(input.sourceId, "Source health centre", { minimum: 0 });
  const destinationId = integer(input.destinationId, "Destination health centre", { minimum: 0 });
  const medicineId = integer(input.medicineId, "Medicine", { minimum: 0 });
  const quantity = integer(input.quantity, "Quantity", { minimum: 1 });
  if (sourceId === destinationId) {
    throw new BusinessError("Source and destination health centres must be different.");
  }
  await runAtomic(async (session) => {
    await requireDocument("healthcentres", sourceId, "Source health centre", session);
    await requireDocument("healthcentres", destinationId, "Destination health centre", session);
    await requireDocument("medicines", medicineId, "Medicine", session);
    const source = await db.collection("stockrecords").findOne(
      { centreId: sourceId, medicineId },
      sessionOptions(session),
    );
    if (!source) throw new BusinessError("The source health centre has no record for this medicine.");
    await takeStock(session, sourceId, medicineId, quantity);
    await putStock(session, destinationId, medicineId, quantity, source.expiry);
    await logMovement(session, "TRANSFER", medicineId, quantity, {
      fromCentreId: sourceId,
      toCentreId: destinationId,
    });
  });
}

/** Prove that a simulated mid-transfer failure leaves replica-set stock unchanged. */
async function demonstrateRollback(input) {
  if (!transactionsSupported) {
    throw new BusinessError("Rollback demonstration requires MongoDB replica-set transactions.");
  }
  const sourceId = integer(input.sourceId, "Source health centre", { minimum: 0 });
  const medicineId = integer(input.medicineId, "Medicine", { minimum: 0 });
  const quantity = integer(input.quantity, "Quantity", { minimum: 1 });
  const sourceFilter = { centreId: sourceId, medicineId };
  const before = await db.collection("stockrecords").findOne(sourceFilter);
  if (!before) throw new BusinessError("The selected source medicine has no stock record.");
  const marker = new Error("Intentional rollback test failure.");
  try {
    await runAtomic(async (session) => {
      await takeStock(session, sourceId, medicineId, quantity);
      throw marker;
    });
  } catch (error) {
    if (error !== marker) throw error;
  }
  const after = await db.collection("stockrecords").findOne(sourceFilter);
  if (!after || after.quantity !== before.quantity) {
    throw new Error("Rollback verification failed: stock quantity changed.");
  }
  return { rolledBack: true, quantity: after.quantity };
}

/** Create a stock record and log its initial quantity as a receipt. */
async function createStock(input) {
  const centreId = integer(input.centreId, "Health centre", { minimum: 0 });
  const medicineId = integer(input.medicineId, "Medicine", { minimum: 0 });
  const quantity = integer(input.quantity, "Quantity", { minimum: 0 });
  const expiry = dateValue(input.expiry, "Expiry date");
  await runAtomic(async (session) => {
    await requireDocument("healthcentres", centreId, "Health centre", session);
    await requireDocument("medicines", medicineId, "Medicine", session);
    await db.collection("stockrecords").insertOne({
      centreId,
      medicineId,
      quantity,
      expiry,
      recorded: now(),
    }, sessionOptions(session));
    await logMovement(session, "RECEIVE", medicineId, quantity, {
      centreId,
      note: "Initial stock record",
    });
  });
}

/** Update stock quantities and expiry dates and log quantity adjustments. */
async function updateStock(centreId, medicineId, input) {
  const quantity = integer(input.quantity, "Quantity", { minimum: 0 });
  const expiry = dateValue(input.expiry, "Expiry date");
  await runAtomic(async (session) => {
    const filter = { centreId, medicineId };
    const record = await db.collection("stockrecords").findOne(filter, sessionOptions(session));
    if (!record) throw new BusinessError("Stock record not found.");
    const result = await db.collection("stockrecords").updateOne(
      filter,
      { $set: { quantity, expiry, recorded: now() } },
      sessionOptions(session),
    );
    if (!result.matchedCount) throw new BusinessError("Stock record no longer exists.");
    if (record.quantity !== quantity) {
      await logMovement(session, "ADJUST", medicineId, Math.abs(quantity - record.quantity), {
        centreId,
        note: `Manual adjustment ${record.quantity} -> ${quantity}`,
      });
    }
  });
}

/** Remove an entity and dependent documents inside a transaction when possible. */
async function deleteEntity(name, id) {
  const collection = entityCollection(name);
  await runAtomic(async (session) => {
    await findRequired(name, id, name.slice(0, -1), session);
    if (name === "healthcentres") {
      await db.collection("stockrecords").deleteMany({ centreId: id }, sessionOptions(session));
    } else if (name === "medicines") {
      await db.collection("stockrecords").deleteMany({ medicineId: id }, sessionOptions(session));
      await db.collection("suppliers").updateMany(
        {},
        { $pull: { supplies: id } },
        sessionOptions(session),
      );
    }
    await collection.deleteOne({ _id: id }, sessionOptions(session));
  });
}

/** Route the supported JSON API requests to database operations. */
async function routeApi(request, response, url) {
  const method = request.method;
  const parts = url.pathname.split("/").filter(Boolean).slice(1);
  const body = ["POST", "PUT", "PATCH", "DELETE"].includes(method)
    ? await readJson(request)
    : {};

  if (method === "GET" && parts.length === 1 && parts[0] === "health") {
    return sendJson(response, 200, { connected: true, database: DB_NAME, transactionsSupported });
  }
  if (method === "GET" && parts.length === 1 && parts[0] === "dashboard") {
    return sendJson(response, 200, await dashboardData());
  }
  if (method === "GET" && parts[0] === "reports" && parts[1] === "nearest") {
    const medicineId = integer(url.searchParams.get("medicineId"), "Medicine", { minimum: 0 });
    const longitude = coordinate(url.searchParams.get("longitude"), "Longitude", -180, 180);
    const latitude = coordinate(url.searchParams.get("latitude"), "Latitude", -90, 90);
    const result = await db.collection("healthcentres").aggregate([
      {
        $geoNear: {
          near: { type: "Point", coordinates: [longitude, latitude] },
          distanceField: "distanceMeters",
          spherical: true,
        },
      },
      {
        $lookup: {
          from: "stockrecords",
          let: { centreId: "$_id" },
          pipeline: [{
            $match: {
              $expr: {
                $and: [
                  { $eq: ["$centreId", "$$centreId"] },
                  { $eq: ["$medicineId", medicineId] },
                  { $gt: ["$quantity", 0] },
                ],
              },
            },
          }],
          as: "availableStock",
        },
      },
      { $match: { "availableStock.0": { $exists: true } } },
      { $unwind: "$availableStock" },
      {
        $project: {
          _id: 1,
          name: 1,
          district: 1,
          distanceKm: { $divide: ["$distanceMeters", 1000] },
          quantity: "$availableStock.quantity",
        },
      },
    ]).toArray();
    return sendJson(response, 200, result);
  }
  if (method === "GET" && parts[0] === "movements") {
    const movements = await db.collection("stock_movements")
      .find({})
      .sort({ at: -1 })
      .limit(100)
      .toArray();
    return sendJson(response, 200, movements);
  }
  if (method === "POST" && parts[0] === "actions") {
    if (parts[1] === "dispense") await dispenseMedicine(body);
    else if (parts[1] === "receive") await receiveDelivery(body);
    else if (parts[1] === "transfer") await transferStock(body);
    else if (parts[1] === "rollback-demo") {
      return sendJson(response, 200, await demonstrateRollback(body));
    }
    else throw new BusinessError("Unknown stock operation.");
    return sendJson(response, 200, { ok: true });
  }
  if (method === "POST" && parts[0] === "seed") {
    await seedDatabase();
    return sendJson(response, 200, { ok: true });
  }
  if (method === "DELETE" && parts[0] === "reset") {
    await db.dropDatabase();
    await setupDatabase();
    return sendJson(response, 200, { ok: true });
  }

  const collectionName = parts[0];
  if (collectionName === "stockrecords") {
    const collection = db.collection(collectionName);
    if (method === "GET" && parts.length === 1) {
      const all = decorateStock(await collection.aggregate(stockPipeline()).toArray());
      const filter = url.searchParams.get("filter") || "all";
      const search = (url.searchParams.get("search") || "").trim();
      const filtered = all.filter((record) => {
        const filterMatch = filter === "all" || record.flags.includes(filter.toUpperCase());
        const searchTerm = search.toLocaleLowerCase();
        const searchMatch = !searchTerm
          || record.centre.toLocaleLowerCase().includes(searchTerm)
          || record.medicine.toLocaleLowerCase().includes(searchTerm);
        return filterMatch && searchMatch;
      });
      return sendJson(response, 200, filtered);
    }
    if (method === "POST" && parts.length === 1) {
      await createStock(body);
      return sendJson(response, 201, { ok: true });
    }
    if (parts.length === 2) {
      const [centreIdText, medicineIdText] = parts[1].split(":");
      const centreId = integer(centreIdText, "Health centre", { minimum: 0 });
      const medicineId = integer(medicineIdText, "Medicine", { minimum: 0 });
      if (method === "PATCH") await updateStock(centreId, medicineId, body);
      else if (method === "DELETE") {
        const result = await collection.deleteOne({ centreId, medicineId });
        if (!result.deletedCount) throw new BusinessError("Stock record not found.");
      } else if (kind === "rollback-demo") {
        title = "Test transaction rollback"; route = "actions/rollback-demo";
        definitions = [
          {key:"sourceId",label:"Source centre",type:"select",options:data.centres.map(function (r) { return {value:r._id,label:r.name}; })},
          {key:"medicineId",label:"Medicine",type:"select",options:data.medicines.map(function (r) { return {value:r._id,label:r.name}; })},
          {key:"quantity",label:"Quantity to temporarily remove",type:"number"}
        ];
      } else {
        throw new BusinessError("Unsupported stock-record operation.");
      }
      return sendJson(response, 200, { ok: true });
    }
  }

  if (Object.hasOwn(ID_BASE, collectionName)) {
    const collection = entityCollection(collectionName);
    if (method === "GET" && parts.length === 1) {
      const search = (url.searchParams.get("search") || "").trim();
      const filter = search ? { name: escapedRegex(search) } : {};
      return sendJson(response, 200, await collection.find(filter).sort({ _id: 1 }).toArray());
    }
    if (parts.length === 2) {
      const id = integer(parts[1], "Record ID", { minimum: 0 });
      if (method === "GET") {
        const document = await collection.findOne({ _id: id });
        if (!document) throw new BusinessError("Record not found.");
        return sendJson(response, 200, document);
      }
      if (method === "PATCH") {
        const current = await collection.findOne({ _id: id });
        if (!current) throw new BusinessError("Record not found.");
        const builders = {
          healthcentres: buildHealthCentre,
          medicines: buildMedicine,
          suppliers: buildSupplier,
        };
        const updated = builders[collectionName](body);
        if (collectionName === "suppliers") {
          const missing = await collectionNameForMedicineIds(updated.supplies);
          if (missing.length) {
            throw new BusinessError(`Unknown medicine ID(s): ${missing.join(", ")}.`);
          }
        }
        await collection.updateOne({ _id: id }, { $set: updated });
        return sendJson(response, 200, { ok: true });
      }
      if (method === "DELETE") {
        await deleteEntity(collectionName, id);
        return sendJson(response, 200, { ok: true });
      }
    }
    if (method === "POST" && parts.length === 1) {
      const builders = {
        healthcentres: buildHealthCentre,
        medicines: buildMedicine,
        suppliers: buildSupplier,
      };
      const document = builders[collectionName](body);
      if (collectionName === "suppliers") {
        const missing = await collectionNameForMedicineIds(document.supplies);
        if (missing.length) {
          throw new BusinessError(`Unknown medicine ID(s): ${missing.join(", ")}.`);
        }
      }
      document._id = await nextId(collectionName);
      await collection.insertOne(document);
      return sendJson(response, 201, { ok: true, id: document._id });
    }
  }
  throw new BusinessError("The requested API route does not exist.");
}

/** Return medicine IDs that are not present in the medicine catalogue. */
async function collectionNameForMedicineIds(ids) {
  if (!ids.length) return [];
  const found = await db.collection("medicines")
    .find({ _id: { $in: ids } }, { projection: { _id: 1 } })
    .toArray();
  const valid = new Set(found.map((medicine) => medicine._id));
  return ids.filter((id) => !valid.has(id));
}

/** Serve the app shell or dispatch a same-origin JSON API request. */
async function handleRequest(request, response) {
  try {
    const url = new URL(request.url, `http://${HOST}:${PORT}`);
    if (url.pathname === "/" && request.method === "GET") {
      return sendApp(response);
    }
    if (url.pathname.startsWith("/api/")) {
      return await routeApi(request, response, url);
    }
    return sendJson(response, 404, { error: "Not found." });
  } catch (error) {
    const normalized = normalizeError(error);
    if (normalized.status >= 500) {
      console.error("Request failed:", error);
    }
    if (!response.headersSent) {
      sendJson(response, normalized.status, { error: normalized.message });
    } else {
      response.destroy(error);
    }
  }
}

/** Start the HTTP server after MongoDB schema and index setup succeeds. */
async function start() {
  try {
    await connectDatabase();
  } catch (error) {
    console.error(`Could not connect to MongoDB at ${MONGO_URI}:`, error.message);
    process.exitCode = 1;
    return;
  }

  const server = http.createServer(handleRequest);
  server.on("error", (error) => {
    console.error("HTTP server error:", error);
    process.exitCode = 1;
  });
  server.listen(PORT, HOST, () => {
    console.log(`Medicine Tracking System running at http://${HOST}:${PORT}`);
    console.log(`MongoDB database: ${DB_NAME}`);
    console.log(
      transactionsSupported
        ? "MongoDB replica set detected; multi-document transactions are enabled."
        : "Standalone MongoDB detected; multi-document rollback is unavailable.",
    );
  });

  const shutdown = async () => {
    server.close();
    if (client) await client.close();
  };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
}

const APP_HTML = String.raw`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MedTrack · Medicine Operations</title>
  <style>
    :root{font-family:Inter,"Segoe UI",Arial,sans-serif;color:#172b45;background:#f4f7fb;font-synthesis:none}
    *{box-sizing:border-box}body{margin:0;min-width:360px}.shell{min-height:100vh;display:flex}
    .sidebar{width:242px;background:#14243a;color:white;padding:25px 13px;display:flex;flex-direction:column;flex-shrink:0}
    .brand{display:flex;align-items:center;gap:12px;padding:2px 12px 32px}.brand-icon{font-size:29px;color:#12b8a6}
    .brand strong{display:block;font-size:14px;letter-spacing:1px}.brand small{font-size:8px;color:#91a5ba;letter-spacing:1px}
    .nav-title{padding:0 12px 9px;color:#72869d;font-size:9px;font-weight:800;letter-spacing:1px}
    .nav{border:0;background:transparent;color:#bac6d4;text-align:left;width:100%;padding:12px 14px;border-radius:9px;margin:2px 0;font-size:13px;cursor:pointer}
    .nav:hover,.nav.active{background:#203750;color:white}.nav.active{box-shadow:inset 3px 0 #12b8a6}
    .side-bottom{margin-top:auto;border-top:1px solid #34465b;padding:15px 11px 0;color:#d2dce7;font-size:10px}
    .side-bottom small{display:block;color:#8fa3b9;font-size:9px;font-weight:800;margin-bottom:7px}
    main{min-width:0;flex:1;display:flex;flex-direction:column}.topbar{height:74px;background:white;display:flex;align-items:center;justify-content:space-between;padding:0 30px}
    h1{font-size:19px;margin:0;color:#172b45}.clock{font-size:12px;color:#78869a}.status-pill{background:#e8f7f3;color:#187f6a;padding:9px 13px;border-radius:20px;font-size:11px;font-weight:700}
    .content{padding:25px 30px;flex:1;overflow:auto}.footer{padding:10px 30px 15px;color:#78869a;font-size:11px;display:flex;justify-content:space-between}
    .page{display:none}.page.active{display:block}.page-intro{margin-bottom:20px}.page-intro h2{margin:0;font-size:24px}.page-intro p{margin:6px 0 0;color:#78869a;font-size:13px}
    .toolbar{display:flex;align-items:center;gap:9px;flex-wrap:wrap;margin-bottom:15px}.toolbar .spacer{flex:1}
    button,.button{border:0;border-radius:8px;background:#fff;color:#354b64;padding:10px 13px;font-size:12px;font-weight:650;cursor:pointer;box-shadow:0 1px 3px #16283a12}
    button:hover{filter:brightness(.97)}button.primary{background:#12a998;color:#fff}button.danger{color:#bd4d59}
    input,select{border:1px solid #dfe6ef;border-radius:8px;background:white;padding:10px 11px;color:#172b45;font:inherit;font-size:12px}
    input:focus,select:focus{outline:2px solid #12b8a633;border-color:#12b8a6}
    .cards{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:13px;margin:16px 0}
    .card,.panel{background:#fff;border:1px solid #e6ebf2;border-radius:12px;padding:17px}
    .metric-icon{width:37px;height:34px;border-radius:9px;display:grid;place-items:center;color:#fff;font-size:17px;font-weight:800}
    .metric-title{font-size:9px;letter-spacing:.8px;color:#78869a;font-weight:800;margin-top:13px}
    .metric-value{font-size:27px;font-weight:800;margin-top:3px}.metric-detail{font-size:10px;color:#78869a;margin-top:5px}
    .dashboard-grid{display:grid;grid-template-columns:minmax(0,1.35fr) minmax(330px,1fr);gap:14px}
    .panel h3{margin:0;font-size:14px}.sub{color:#8491a2;font-size:11px;margin:5px 0 15px}
    .graph-scroll{overflow:auto;max-height:400px}.chart-row{display:grid;grid-template-columns:minmax(105px,155px) minmax(90px,1fr) 65px;align-items:center;gap:10px;margin:9px 0;font-size:11px}
    .chart-label{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#52647a}.chart-track{height:12px;background:#eef2f7;border-radius:20px;overflow:hidden}
    .chart-bar{height:100%;border-radius:20px;min-width:2px}.chart-value{text-align:right;font-weight:700}
    .table-wrap{overflow:auto;background:white;border:1px solid #e6ebf2;border-radius:11px}
    table{width:100%;border-collapse:collapse;white-space:nowrap;font-size:11px}thead{background:#f2f5f9;color:#64758a}
    th{text-align:left;padding:12px 13px;font-size:9px;letter-spacing:.5px}td{padding:11px 13px;border-top:1px solid #edf1f5}
    tr:hover td{background:#f8fafc}.badge{display:inline-block;border-radius:20px;padding:4px 8px;font-size:9px;font-weight:800;background:#eef2f7;color:#627187}
    .badge.low,.badge.expired{background:#fff0ef;color:#bc4c55}.badge.expiring{background:#fff5df;color:#9a6b1f}
    .row-actions{display:flex;gap:5px}.row-actions button{padding:6px 8px;font-size:10px}
    .empty{text-align:center;color:#8996a7;padding:26px}.alert-line{display:flex;gap:8px;align-items:center;padding:9px 0;border-top:1px solid #edf1f5;font-size:11px}
    .operation-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}
    .operation{display:flex;align-items:center;gap:14px}.operation-icon{width:44px;height:44px;border-radius:11px;color:white;display:grid;place-items:center;font-size:20px;font-weight:800}
    .operation h3{margin:0 0 5px;font-size:14px}.operation p{margin:0;color:#78869a;font-size:11px;line-height:1.5}.operation button{margin-left:auto;white-space:nowrap}
    .notice{padding:12px 14px;border-radius:9px;background:#fff5df;color:#87631e;font-size:11px;margin-top:14px}
    .reports-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
    .report-card{display:flex;gap:12px;align-items:center}.report-stripe{width:5px;align-self:stretch;border-radius:5px}
    .report-card .copy{flex:1}.report-card h3{margin:0 0 5px;font-size:13px}.report-card p{margin:0;color:#78869a;font-size:11px}
    dialog{border:0;border-radius:14px;padding:0;width:min(520px,calc(100% - 30px));box-shadow:0 20px 70px #14243a44}
    dialog::backdrop{background:#14243a88}.dialog-head{padding:18px 20px;border-bottom:1px solid #e9edf3;display:flex;justify-content:space-between;align-items:center}
    .dialog-head h2{margin:0;font-size:17px}.dialog-body{padding:18px 20px;max-height:65vh;overflow:auto}
    .field{display:grid;gap:6px;margin-bottom:13px}.field label{font-size:11px;color:#53657a;font-weight:700}
    .field input,.field select{width:100%}.field select[multiple]{height:125px}.dialog-actions{display:flex;justify-content:flex-end;gap:8px;padding:13px 20px;border-top:1px solid #e9edf3}
    .toast{position:fixed;right:24px;bottom:24px;background:#172b45;color:white;padding:13px 17px;border-radius:10px;font-size:12px;box-shadow:0 8px 30px #15243b44;z-index:5;max-width:min(440px,calc(100% - 30px))}
    .toast.error{background:#ad3d49}.hidden{display:none!important}.loading{opacity:.6;pointer-events:none}
    @media(max-width:1050px){.sidebar{width:205px}.dashboard-grid{grid-template-columns:1fr}.cards{grid-template-columns:repeat(2,minmax(0,1fr))}}
    @media(max-width:700px){.sidebar{width:65px;padding:15px 7px}.brand{justify-content:center;padding:5px 0 20px}.brand-text,.nav-title,.side-bottom,.nav .nav-text{display:none}.nav{font-size:18px;text-align:center;padding:12px 4px}.topbar{padding:0 14px}.content{padding:18px 13px}.clock{display:none}.operation-grid,.reports-grid{grid-template-columns:1fr}.cards{gap:8px}.card{padding:12px}.metric-value{font-size:22px}}
  </style>
</head>
<body>
  <div class="shell">
    <aside class="sidebar">
      <div class="brand"><div class="brand-icon">✚</div><div class="brand-text"><strong>MEDTRACK</strong><small>PHARMACY OPERATIONS</small></div></div>
      <div class="nav-title">WORKSPACE</div>
      <button class="nav active" data-page="dashboard">⌂ <span class="nav-text"> Overview</span></button>
      <button class="nav" data-page="healthcentres">＋ <span class="nav-text"> Health centres</span></button>
      <button class="nav" data-page="medicines">✚ <span class="nav-text"> Medicines</span></button>
      <button class="nav" data-page="suppliers">▤ <span class="nav-text"> Suppliers</span></button>
      <button class="nav" data-page="stockrecords">▦ <span class="nav-text"> Stock & inventory</span></button>
      <button class="nav" data-page="operations">↔ <span class="nav-text"> Stock operations</span></button>
      <button class="nav" data-page="reports">◷ <span class="nav-text"> Reports & insights</span></button>
      <div class="side-bottom"><small>DATABASE</small><span id="database-name">Connecting…</span></div>
    </aside>
    <main>
      <header class="topbar"><h1 id="page-title">Overview</h1><span id="clock" class="clock"></span><span id="connection" class="status-pill">Connecting…</span></header>
      <div class="content">
        <section class="page active" id="page-dashboard">
          <div class="page-intro"><h2>Good to see you</h2><p>Your medicine network at a glance. Inventory and the chart update when you refresh.</p></div>
          <div class="toolbar">
            <button class="primary" data-action="create-medicine">＋ Add medicine</button>
            <button data-action="receive">⇧ Receive delivery</button>
            <button data-action="dispense">− Dispense</button>
            <span class="spacer"></span>
            <button data-action="refresh">↻ Refresh graph</button>
            <button data-action="export-stock">⇩ Export inventory</button>
          </div>
          <div class="cards" id="metrics"></div>
          <div class="dashboard-grid">
            <div class="panel"><h3>Inventory by medicine</h3><div class="sub">Every catalogue item, including medicines with zero stock</div><div id="chart" class="graph-scroll"></div></div>
            <div class="panel"><h3>Stock alerts <button data-page="stockrecords" style="float:right;padding:3px 5px;color:#4389f5;box-shadow:none">View stock →</button></h3><div class="sub">Low stock, expired, and upcoming expiry</div><div id="alerts"></div></div>
          </div>
          <div class="panel" style="margin-top:14px"><h3>Recent movements</h3><div class="sub">Latest stock activity across health centres</div><div id="recent-movements"></div></div>
        </section>
        <section class="page" id="page-healthcentres"></section>
        <section class="page" id="page-medicines"></section>
        <section class="page" id="page-suppliers"></section>
        <section class="page" id="page-stockrecords"></section>
        <section class="page" id="page-operations"></section>
        <section class="page" id="page-reports"></section>
      </div>
      <footer class="footer"><span id="message">Ready</span><button data-action="refresh" style="padding:5px 8px;box-shadow:none">↻ Refresh data</button></footer>
    </main>
  </div>
  <dialog id="dialog"><form id="dialog-form"><div class="dialog-head"><h2 id="dialog-title"></h2><button type="button" data-action="close-dialog" aria-label="Close">✕</button></div><div class="dialog-body" id="dialog-fields"></div><div class="dialog-actions"><button type="button" data-action="close-dialog">Cancel</button><button type="submit" class="primary">Save</button></div></form></dialog>
  <div id="toast" class="toast hidden" role="status"></div>
  <script>
    "use strict";
    const state = { dashboard: null, stockFilter: "all", stockSearch: "", search: {}, page: "dashboard" };
    const colors = ["#12b8a6","#4389f5","#8665e8","#f0a23a","#e36a8d","#43a6a0","#7190cc"];
    const pageNames = {dashboard:"Overview",healthcentres:"Health centres",medicines:"Medicine catalogue",suppliers:"Supplier directory",stockrecords:"Stock & inventory",operations:"Stock operations",reports:"Reports & insights"};
    const entityConfig = {
      healthcentres: {label:"Health centres",columns:[["_id","ID"],["name","Name"],["district","District"],["level","Level"]],fields:[
        ["name","Name","text"],["district","District","text"],["level","Level","select",["HC II","HC III","HC IV","Hospital"]],["longitude","Longitude","number"],["latitude","Latitude","number"]]},
      medicines: {label:"Medicines",columns:[["_id","ID"],["name","Name"],["category","Category"],["unit","Type"],["minLevel","Minimum quantity"]],fields:[
        ["name","Medicine name","text"],["category","Category","select",["Antimalarial","Antibiotic","Painkiller","Rehydration"]],["unit","Medicine type","select",["tablets","capsules","sachets"]],["minLevel","Minimum quantity","number"]]},
      suppliers: {label:"Suppliers",columns:[["_id","ID"],["name","Name"],["contact","Contact"],["supplies","Medicines supplied"]],fields:[
        ["name","Supplier name","text"],["contact","Contact details","text"],["supplies","Medicines supplied","multi","medicines"]]},
      stockrecords: {label:"Stock & inventory",columns:[["centre","Health centre"],["medicine","Medicine"],["quantity","Quantity"],["minLevel","Minimum"],["expiry","Expiry"],["flags","Status"]]}
    };
    function escapeHtml(value) {
      return String(value == null ? "" : value).replace(/[&<>"']/g, function (char) {
        return {"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[char];
      });
    }
    function displayDate(value) {
      if (!value) return "—";
      const date = new Date(value);
      return Number.isNaN(date.getTime()) ? "—" : date.toLocaleDateString();
    }
    async function api(path, options) {
      const settings = options || {};
      settings.headers = Object.assign({"content-type":"application/json"}, settings.headers || {});
      const response = await fetch("/api/" + path, settings);
      const payload = await response.json().catch(function () { return {}; });
      if (!response.ok) throw new Error(payload.error || "Request failed.");
      return payload;
    }
    function toast(message, error) {
      const element = document.getElementById("toast");
      element.textContent = message;
      element.className = "toast" + (error ? " error" : "");
      window.clearTimeout(toast.timer);
      toast.timer = window.setTimeout(function () { element.className = "toast hidden"; }, 3800);
      document.getElementById("message").textContent = message;
    }
    function withError(action) {
      return Promise.resolve().then(action).catch(function (error) { toast(error.message, true); });
    }
    function navigate(page) {
      if (!pageNames[page]) return;
      state.page = page;
      document.querySelectorAll(".page").forEach(function (element) {
        element.classList.toggle("active", element.id === "page-" + page);
      });
      document.querySelectorAll(".nav").forEach(function (element) {
        element.classList.toggle("active", element.dataset.page === page);
      });
      document.getElementById("page-title").textContent = pageNames[page];
      if (page === "dashboard") refreshData();
      if (page === "stockrecords") loadStock();
      if (page in entityConfig && page !== "stockrecords") loadEntity(page);
    }
    function updateClock() {
      document.getElementById("clock").textContent = new Date().toLocaleString([], {weekday:"long",day:"numeric",month:"long",hour:"2-digit",minute:"2-digit"});
    }
    function badgeFlags(flags) {
      return (flags || []).length ? flags.map(function (flag) {
        const kind = flag.toLowerCase();
        return '<span class="badge ' + escapeHtml(kind) + '">' + escapeHtml(flag) + '</span>';
      }).join(" ") : '<span class="badge">OK</span>';
    }
    function tableHtml(columns, rows, actions) {
      if (!rows.length) return '<div class="empty">No records found.</div>';
      const head = columns.map(function (column) { return "<th>" + escapeHtml(column[1]) + "</th>"; }).join("");
      const body = rows.map(function (row) {
        const cells = columns.map(function (column) {
          const value = row[column[0]];
          return "<td>" + (column[0] === "flags" ? badgeFlags(value) : escapeHtml(Array.isArray(value) ? value.join(", ") : value)) + "</td>";
        }).join("");
        const actionCell = actions ? '<td><div class="row-actions">' + actions(row) + "</div></td>" : "";
        return "<tr>" + cells + actionCell + "</tr>";
      }).join("");
      return '<div class="table-wrap"><table><thead><tr>' + head + (actions ? "<th>Actions</th>" : "") + "</tr></thead><tbody>" + body + "</tbody></table></div>";
    }
    function entityTable(resource, rows) {
      const config = entityConfig[resource];
      const actions = function (row) {
        return '<button data-action="view-entity" data-resource="' + resource + '" data-id="' + row._id + '">View</button><button data-action="edit-entity" data-resource="' + resource + '" data-id="' + row._id + '">Edit</button><button class="danger" data-action="delete-entity" data-resource="' + resource + '" data-id="' + row._id + '">Delete</button>';
      };
      return tableHtml(config.columns, rows, actions);
    }
    function renderEntityPage(resource, records) {
      const config = entityConfig[resource];
      const page = document.getElementById("page-" + resource);
      const search = escapeHtml(state.search[resource] || "");
      const displayRecords = resource === "suppliers" && state.dashboard
        ? records.map(function (record) {
          const names = new Map(state.dashboard.medicines.map(function (medicine) {
            return [medicine._id, medicine.name];
          }));
          return Object.assign({}, record, {
            supplies: record.supplies.map(function (id) { return names.get(id) || "Unknown medicine"; }),
          });
        })
        : records;
      page.innerHTML = '<div class="page-intro"><h2>' + escapeHtml(config.label) + '</h2><p>Search, review, add, update, and remove ' + escapeHtml(config.label.toLowerCase()) + '.</p></div><div class="toolbar"><input data-search="' + resource + '" value="' + search + '" placeholder="Search by name"><span class="spacer"></span><button data-action="export-entity" data-resource="' + resource + '">⇩ Export CSV</button><button class="primary" data-action="create-entity" data-resource="' + resource + '">＋ Add new</button></div><div id="table-' + resource + '">' + entityTable(resource, displayRecords) + "</div>";
    }
    async function loadEntity(resource) {
      const query = state.search[resource] || "";
      const rows = await api(resource + "?search=" + encodeURIComponent(query));
      renderEntityPage(resource, rows);
    }
    function stockColumns() {
      return entityConfig.stockrecords.columns;
    }
    async function loadStock() {
      const query = new URLSearchParams({filter:state.stockFilter,search:state.stockSearch});
      const rows = await api("stockrecords?" + query.toString());
      const page = document.getElementById("page-stockrecords");
      const filter = '<select data-stock-filter><option value="all">All stock</option><option value="low">Low stock</option><option value="expiring">Expiring soon</option><option value="expired">Expired</option></select>';
      const table = tableHtml(stockColumns(), rows.map(function (row) {
        return Object.assign({}, row, {expiry:displayDate(row.expiry)});
      }), function (row) {
        return '<button data-action="view-stock" data-centre-id="' + row.centreId + '" data-medicine-id="' + row.medicineId + '">View</button><button data-action="edit-stock" data-centre-id="' + row.centreId + '" data-medicine-id="' + row.medicineId + '">Adjust</button><button class="danger" data-action="delete-stock" data-centre-id="' + row.centreId + '" data-medicine-id="' + row.medicineId + '">Delete</button>';
      });
      page.innerHTML = '<div class="page-intro"><h2>Stock & inventory</h2><p>Search by medicine or centre and filter low, expiring, or expired stock.</p></div><div class="toolbar"><input data-stock-search value="' + escapeHtml(state.stockSearch) + '" placeholder="Search medicine / centre">' + filter + '<span class="spacer"></span><button data-action="export-stock">⇩ Export CSV</button><button class="primary" data-action="create-stock">＋ Add stock</button></div>' + table;
      page.querySelector("[data-stock-filter]").value = state.stockFilter;
    }
    function renderMetrics(totals) {
      const metrics = [
        ["Health centres",totals.centres,totals.suppliers + " registered suppliers","#4389f5","⌂"],
        ["Medicine types",totals.medicines,totals.stockRecords + " stock records","#8665e8","✚"],
        ["Units in stock",totals.units.toLocaleString(),totals.low + " low-stock alerts","#12b8a6","▦"],
        ["Attention needed",totals.alerts,totals.low + " low · " + totals.expired + " expired · " + totals.expiring + " expiring","#f0a23a","!"]
      ];
      document.getElementById("metrics").innerHTML = metrics.map(function (item) {
        return '<div class="card"><div class="metric-icon" style="background:' + item[3] + '">' + item[4] + '</div><div class="metric-title">' + item[0].toUpperCase() + '</div><div class="metric-value">' + item[1] + '</div><div class="metric-detail">' + item[2] + "</div></div>";
      }).join("");
    }
    function renderChart(chart) {
      const target = document.getElementById("chart");
      if (!chart.length) {
        target.innerHTML = '<div class="empty">Add medicines to see them on the inventory graph.</div>';
        return;
      }
      const max = Math.max(1, ...chart.map(function (item) { return item.quantity; }));
      target.innerHTML = chart.map(function (item, index) {
        const width = item.quantity ? Math.max(1, item.quantity / max * 100) : 1;
        return '<div class="chart-row"><div class="chart-label" title="' + escapeHtml(item.name) + '">' + escapeHtml(item.name) + '</div><div class="chart-track"><div class="chart-bar" style="width:' + width + "%;background:" + colors[index % colors.length] + '"></div></div><div class="chart-value">' + item.quantity.toLocaleString() + " " + escapeHtml(item.unit) + "</div></div>";
      }).join("");
    }
    function renderAlerts(alerts) {
      const target = document.getElementById("alerts");
      if (!alerts.length) {
        target.innerHTML = '<div class="empty">All clear — no stock alerts.</div>';
        return;
      }
      target.innerHTML = alerts.slice(0, 9).map(function (row) {
        const status = row.flags.includes("EXPIRED") ? "EXPIRED" : row.flags.includes("LOW") ? "LOW STOCK" : "EXP " + displayDate(row.expiry);
        const color = status === "LOW STOCK" || status === "EXPIRED" ? "#bd4d59" : "#9a6b1f";
        return '<div class="alert-line"><span style="color:' + color + '">●</span><strong>' + escapeHtml(row.medicine) + '</strong><span style="color:#78869a">· ' + escapeHtml(row.centre) + '</span><span class="spacer"></span><span class="badge ' + (color === "#bd4d59" ? "low" : "expiring") + '">' + status + "</span></div>";
      }).join("");
    }
    function renderMovements(movements, targetId) {
      const rows = movements.map(function (movement) {
        return {
          when: new Date(movement.at).toLocaleString(),
          type: movement.type,
          medicineId: movement.medicineId,
          quantity: movement.quantity,
          centre: movement.centreId == null ? (movement.fromCentreId || "") + " → " + (movement.toCentreId || "") : movement.centreId,
          reference: movement.patientRef || movement.note || ""
        };
      });
      const columns = [["when","When"],["type","Movement"],["medicineId","Medicine"],["quantity","Quantity"],["centre","Centre"],["reference","Reference"]];
      document.getElementById(targetId).innerHTML = tableHtml(columns, rows);
    }
    async function refreshData() {
      const data = await api("dashboard");
      state.dashboard = data;
      renderMetrics(data.totals);
      renderChart(data.chart);
      renderAlerts(data.alerts);
      renderMovements(data.movements, "recent-movements");
      document.getElementById("database-name").textContent = data.transactionsSupported ? DB_LABEL + " · ACID enabled" : DB_LABEL + " · standalone mode";
      document.getElementById("connection").textContent = "● Database connected";
      if (state.page === "stockrecords") await loadStock();
      if (state.page in entityConfig && state.page !== "stockrecords") await loadEntity(state.page);
      toast("Dashboard, stock, alerts, and graph refreshed.");
    }
    function fieldsFor(resource, existing) {
      const definitions = entityConfig[resource].fields.map(function (field) {
        return {key:field[0],label:field[1],type:field[2],options:field[3] || []};
      });
      if (resource === "suppliers") {
        definitions[2].options = state.dashboard.medicines.map(function (medicine) {
          return {value:medicine._id,label:medicine.name};
        });
      }
      if (resource === "healthcentres" && existing) {
        existing.longitude = existing.location.coordinates[0];
        existing.latitude = existing.location.coordinates[1];
      }
      return definitions;
    }
    function openForm(title, definitions, existing, onSave) {
      const dialog = document.getElementById("dialog");
      const form = document.getElementById("dialog-form");
      form.querySelector('[type="submit"]').classList.remove("hidden");
      document.getElementById("dialog-title").textContent = title;
      document.getElementById("dialog-fields").innerHTML = definitions.map(function (field) {
        const value = existing && existing[field.key] != null ? existing[field.key] : "";
        let control;
        if (field.type === "select" || field.type === "multi") {
          const multi = field.type === "multi";
          const options = field.options.map(function (option) {
            const optionValue = typeof option === "object" ? option.value : option;
            const optionLabel = typeof option === "object" ? option.label : option;
            const selected = multi ? (value || []).map(String).includes(String(optionValue)) : String(value) === String(optionValue);
            return '<option value="' + escapeHtml(optionValue) + '"' + (selected ? " selected" : "") + ">" + escapeHtml(optionLabel) + "</option>";
          }).join("");
          control = '<select name="' + field.key + '"' + (multi ? " multiple" : "") + (field.key === "supplies" && !multi ? " required" : "") + ">" + (multi ? "" : '<option value="">Choose…</option>') + options + "</select>";
        } else {
          const numeric = field.type === "number";
          const inputType = numeric ? "number" : field.type === "date" ? "date" : "text";
          const inputValue = field.type === "date" && value
            ? String(value).slice(0, 10)
            : value;
          control = '<input name="' + field.key + '" type="' + inputType + '"' + (numeric ? ' step="any"' : "") + ' value="' + escapeHtml(inputValue) + '" required>';
        }
        return '<div class="field"><label>' + escapeHtml(field.label) + "</label>" + control + "</div>";
      }).join("");
      form.onsubmit = function (event) {
        event.preventDefault();
        const values = {};
        definitions.forEach(function (field) {
          const control = form.elements[field.key];
          if (field.type === "multi") {
            values[field.key] = Array.from(control.selectedOptions).map(function (option) { return Number(option.value); });
          } else if (field.type === "number") {
            values[field.key] = Number(control.value);
          } else {
            values[field.key] = control.value;
          }
        });
        dialog.close();
        withError(function () { return onSave(values); });
      };
      dialog.showModal();
    }
    function showDetails(title, record) {
      const dialog = document.getElementById("dialog");
      const form = document.getElementById("dialog-form");
      document.getElementById("dialog-title").textContent = title;
      document.getElementById("dialog-fields").innerHTML =
        '<pre style="white-space:pre-wrap;overflow-wrap:anywhere;font-size:12px;color:#40546b">' +
        escapeHtml(JSON.stringify(record, null, 2)) + "</pre>";
      form.onsubmit = null;
      form.querySelector('[type="submit"]').classList.add("hidden");
      dialog.showModal();
    }
    async function createEntity(resource) {
      if (!state.dashboard) await refreshData();
      openForm("Add " + entityConfig[resource].label.replace(/s$/, ""), fieldsFor(resource), null, async function (values) {
        const result = await api(resource, {method:"POST",body:JSON.stringify(values)});
        toast("Record added" + (result.id ? " (ID " + result.id + ")" : "") + ".");
        await refreshData();
      });
    }
    async function editEntity(resource, id) {
      const record = await api(resource + "/" + id);
      openForm("Edit " + (record.name || entityConfig[resource].label), fieldsFor(resource, record), record, async function (values) {
        await api(resource + "/" + id, {method:"PATCH",body:JSON.stringify(values)});
        toast("Record updated.");
        await refreshData();
      });
    }
    async function deleteEntity(resource, id) {
      if (!window.confirm("Delete this record and its related stock references?")) return;
      await api(resource + "/" + id, {method:"DELETE"});
      toast("Record deleted.");
      await refreshData();
    }
    function dateInputDefault() {
      return new Date(Date.now() - new Date().getTimezoneOffset() * 60000).toISOString().slice(0,10);
    }
    async function referenceOptions() {
      if (!state.dashboard) state.dashboard = await api("dashboard");
      return state.dashboard;
    }
    async function stockForm(existing) {
      const data = await referenceOptions();
      const definitions = existing
        ? [{key:"quantity",label:"Quantity",type:"number"},{key:"expiry",label:"Expiry date",type:"date"}]
        : [
          {key:"centreId",label:"Health centre",type:"select",options:data.centres.map(function (r) { return {value:r._id,label:r.name}; })},
          {key:"medicineId",label:"Medicine",type:"select",options:data.medicines.map(function (r) { return {value:r._id,label:r.name}; })},
          {key:"quantity",label:"Quantity",type:"number"},
          {key:"expiry",label:"Expiry date",type:"date"}
        ];
      openForm(existing ? "Adjust stock" : "Add stock", definitions, existing || {expiry:dateInputDefault()}, async function (values) {
        const route = existing ? "stockrecords/" + existing.centreId + ":" + existing.medicineId : "stockrecords";
        await api(route, {method:existing ? "PATCH" : "POST",body:JSON.stringify(values)});
        toast(existing ? "Stock updated." : "Stock record created.");
        await refreshData();
      });
    }
    async function operationForm(kind) {
      const data = await referenceOptions();
      let title;
      let definitions;
      let route;
      if (kind === "dispense") {
        title = "Dispense medicine"; route = "actions/dispense";
        definitions = [
          {key:"centreId",label:"Health centre",type:"select",options:data.centres.map(function (r) { return {value:r._id,label:r.name}; })},
          {key:"medicineId",label:"Medicine",type:"select",options:data.medicines.map(function (r) { return {value:r._id,label:r.name}; })},
          {key:"quantity",label:"Quantity",type:"number"},{key:"patientRef",label:"Patient reference",type:"text"}
        ];
      } else if (kind === "receive") {
        title = "Receive delivery"; route = "actions/receive";
        definitions = [
          {key:"supplierId",label:"Supplier",type:"select",options:data.suppliers.map(function (r) { return {value:r._id,label:r.name}; })},
          {key:"centreId",label:"Health centre",type:"select",options:data.centres.map(function (r) { return {value:r._id,label:r.name}; })},
          {key:"medicineId",label:"Medicine",type:"select",options:data.medicines.map(function (r) { return {value:r._id,label:r.name}; })},
          {key:"quantity",label:"Quantity received",type:"number"},{key:"expiry",label:"Expiry date",type:"date"}
        ];
      } else {
        title = "Transfer stock"; route = "actions/transfer";
        definitions = [
          {key:"sourceId",label:"Source centre",type:"select",options:data.centres.map(function (r) { return {value:r._id,label:r.name}; })},
          {key:"destinationId",label:"Destination centre",type:"select",options:data.centres.map(function (r) { return {value:r._id,label:r.name}; })},
          {key:"medicineId",label:"Medicine",type:"select",options:data.medicines.map(function (r) { return {value:r._id,label:r.name}; })},
          {key:"quantity",label:"Quantity",type:"number"}
        ];
      }
      if (definitions.some(function (field) { return field.options && field.options.length === 0; })) {
        throw new Error("Add the required medicines, centres, and suppliers before this operation.");
      }
      openForm(title, definitions, kind === "receive" ? {expiry:dateInputDefault()} : null, async function (values) {
        await api(route, {method:"POST",body:JSON.stringify(values)});
        toast(kind === "rollback-demo" ? "Rollback verified; stock remained unchanged." : title + " completed and movement recorded.");
        await refreshData();
      });
    }
    async function showMovements() {
      const movements = await api("movements");
      renderMovements(movements, "movement-history");
    }
    async function createOperationsPage() {
      const page = document.getElementById("page-operations");
      const operations = [
        ["Dispense medicine","Issue stock to a patient and record the reference.","#4389f5","−","dispense"],
        ["Receive delivery","Increase stock from an approved supplier.","#12b8a6","＋","receive"],
        ["Transfer stock","Move stock between health centres and keep an audit trail.","#8665e8","↔","transfer"],
        ["Test transaction rollback","Verify that a failed transfer leaves stock unchanged (replica set required).","#e35d6a","↶","rollback-demo"]
      ];
      page.innerHTML = '<div class="page-intro"><h2>Stock operations</h2><p>Record important stock movements with validation and an audit trail.</p></div><div class="operation-grid">' + operations.map(function (item) {
        return '<div class="panel operation"><div class="operation-icon" style="background:' + item[2] + '">' + item[3] + '</div><div><h3>' + item[0] + '</h3><p>' + item[1] + '</p></div><button data-action="' + item[4] + '">Open →</button></div>';
      }).join("") + '<div class="panel operation"><div class="operation-icon" style="background:#f0a23a">◷</div><div><h3>Movement history</h3><p>Review the latest 100 inventory movement records.</p></div><button data-action="movements">Open →</button></div></div><div class="notice">ACID transactions require MongoDB to run as a replica set. Stock updates are still supported in standalone mode, but multi-document rollback is unavailable.</div><div class="panel" style="margin-top:15px"><h3>Movement history</h3><div class="sub">Latest 100 audit records</div><div id="movement-history"><div class="empty">Choose Movement history to load records.</div></div></div>';
    }
    function reportsPage() {
      const reports = [
        ["All inventory","Stock position for each medicine and health centre.","#4389f5","all-stock"],
        ["Reorder watchlist","Find stock below its configured minimum quantity.","#e35d6a","low-stock"],
        ["Expiry forecast","Filter stock expiring within a chosen number of days.","#f0a23a","expiring"],
        ["Nearby availability","Find centres with stock near a geographic location.","#12b8a6","nearest"]
      ];
      document.getElementById("page-reports").innerHTML = '<div class="page-intro"><h2>Reports & insights</h2><p>Review stock risk and export operational data.</p></div><div class="reports-grid">' + reports.map(function (item) {
        return '<div class="panel report-card"><div class="report-stripe" style="background:' + item[2] + '"></div><div class="copy"><h3>' + item[0] + '</h3><p>' + item[1] + '</p></div><button data-action="' + item[3] + '">Run →</button></div>';
      }).join("") + '</div><div class="toolbar" style="margin-top:15px"><button data-action="seed">Load sample data</button><button class="danger" data-action="reset">Reset database</button></div><div id="report-output"></div>';
    }
    function renderReport(title, columns, rows) {
      const target = document.getElementById("report-output");
      target.innerHTML = '<div class="panel" style="margin-top:12px"><div class="toolbar"><h3 style="margin:0">' + escapeHtml(title) + " · " + rows.length + ' records</h3><span class="spacer"></span><button data-action="export-report">⇩ Export CSV</button></div><div id="report-table">' + tableHtml(columns, rows) + "</div></div>";
      state.report = {title:title,columns:columns,rows:rows};
    }
    async function runReport(kind) {
      if (kind === "nearest") {
        const data = await referenceOptions();
        openForm("Find nearby medicine", [
          {key:"medicineId",label:"Medicine",type:"select",options:data.medicines.map(function (r) { return {value:r._id,label:r.name}; })},
          {key:"longitude",label:"Longitude",type:"number"},
          {key:"latitude",label:"Latitude",type:"number"}
        ], {longitude:32.50,latitude:0.70}, async function (values) {
          const query = new URLSearchParams(values);
          const rows = await api("reports/nearest?" + query.toString());
          renderReport("Nearby centres with medicine",[["_id","ID"],["name","Health centre"],["district","District"],["distanceKm","Distance (km)"],["quantity","In stock"]],rows.map(function (row) { return Object.assign({},row,{distanceKm:row.distanceKm.toFixed(1)}); }));
        });
        return;
      }
      if (kind === "expiring") {
        const days = window.prompt("Show medicines expiring within how many days?", "90");
        if (days === null) return;
        const parsed = Number(days);
        if (!Number.isSafeInteger(parsed) || parsed < 1) throw new Error("Enter a positive whole number of days.");
        const cutoff = Date.now() + parsed * 86400000;
        const rows = state.dashboard.stock.filter(function (row) { return new Date(row.expiry).getTime() <= cutoff; });
        renderReport("Expiring within " + parsed + " days",stockColumns(),rows.map(function (row) { return Object.assign({},row,{expiry:displayDate(row.expiry)}); }));
        return;
      }
      const rows = kind === "low-stock" ? state.dashboard.stock.filter(function (row) { return row.flags.includes("LOW"); }) : state.dashboard.stock;
      renderReport(kind === "low-stock" ? "Low stock watchlist" : "All inventory",stockColumns(),rows.map(function (row) { return Object.assign({},row,{expiry:displayDate(row.expiry)}); }));
    }
    function downloadCsv(title, columns, rows) {
      const quote = function (value) { return '"' + String(value == null ? "" : value).replace(/"/g, '""') + '"'; };
      const lines = [columns.map(function (column) { return quote(column[1]); }).join(",")];
      rows.forEach(function (row) {
        lines.push(columns.map(function (column) {
          const value = row[column[0]];
          return quote(Array.isArray(value) ? value.join(", ") : value);
        }).join(","));
      });
      const blob = new Blob(["\ufeff" + lines.join("\r\n")], {type:"text/csv;charset=utf-8"});
      const link = document.createElement("a");
      link.href = URL.createObjectURL(blob);
      link.download = title.toLowerCase().replace(/[^a-z0-9]+/g,"-").replace(/^-|-$/g,"") + ".csv";
      link.click();
      URL.revokeObjectURL(link.href);
      toast("CSV export downloaded.");
    }
    async function seedData() {
      await api("seed",{method:"POST",body:"{}"});
      await refreshData();
      toast("Sample records loaded into empty collections.");
    }
    async function resetData() {
      if (!window.confirm("Delete all data in " + DB_LABEL + "? This cannot be undone.")) return;
      await api("reset",{method:"DELETE",body:"{}"});
      await refreshData();
      toast("Database reset.");
    }
    document.addEventListener("click", function (event) {
      const pageControl = event.target.closest("[data-page]");
      if (pageControl) { navigate(pageControl.dataset.page); return; }
      const button = event.target.closest("[data-action]");
      if (!button) return;
      const action = button.dataset.action;
      const resource = button.dataset.resource;
      const id = button.dataset.id;
      withError(async function () {
        if (action === "refresh") await refreshData();
        else if (action === "create-medicine") await createEntity("medicines");
        else if (action === "create-entity") await createEntity(resource);
        else if (action === "view-entity") {
          const record = await api(resource + "/" + id);
          showDetails(entityConfig[resource].label.replace(/s$/, ""),record);
        }
        else if (action === "edit-entity") await editEntity(resource,id);
        else if (action === "delete-entity") await deleteEntity(resource,id);
        else if (action === "create-stock") await stockForm(null);
        else if (action === "view-stock") {
          const centreId = Number(button.dataset.centreId);
          const medicineId = Number(button.dataset.medicineId);
          const record = state.dashboard.stock.find(function (item) { return item.centreId === centreId && item.medicineId === medicineId; });
          if (!record) throw new Error("Stock record not found; refresh and try again.");
          showDetails("Stock record",record);
        }
        else if (action === "edit-stock") {
          const row = state.dashboard.stock.find(function (item) { return item.centreId === Number(button.dataset.centreId) && item.medicineId === Number(button.dataset.medicineId); });
          if (!row) throw new Error("Stock record not found; refresh and try again.");
          await stockForm(row);
        } else if (action === "delete-stock") {
          if (window.confirm("Delete this stock record?")) {
            await api("stockrecords/" + button.dataset.centreId + ":" + button.dataset.medicineId,{method:"DELETE",body:"{}"});
            await refreshData();
            toast("Stock record deleted.");
          }
        }         else if (["dispense","receive","transfer","rollback-demo"].includes(action)) await operationForm(action);
        else if (action === "movements") await showMovements();
        else if (action === "all-stock" || action === "low-stock" || action === "expiring" || action === "nearest") await runReport(action);
        else if (action === "seed") await seedData();
        else if (action === "reset") await resetData();
        else if (action === "export-stock") downloadCsv("inventory",stockColumns(),state.dashboard.stock);
        else if (action === "export-entity") {
          const records = await api(resource);
          downloadCsv(resource,entityConfig[resource].columns,records);
        } else if (action === "export-report" && state.report) downloadCsv(state.report.title,state.report.columns,state.report.rows);
        else if (action === "close-dialog") document.getElementById("dialog").close();
      });
    });
    document.addEventListener("input", function (event) {
      if (event.target.matches("[data-search]")) {
        const resource = event.target.dataset.search;
        state.search[resource] = event.target.value;
        window.clearTimeout(event.target.searchTimer);
        event.target.searchTimer = window.setTimeout(function () { withError(function () { return loadEntity(resource); }); },250);
      }
      if (event.target.matches("[data-stock-search]")) {
        state.stockSearch = event.target.value;
        window.clearTimeout(event.target.searchTimer);
        event.target.searchTimer = window.setTimeout(function () { withError(loadStock); },250);
      }
    });
    document.addEventListener("change", function (event) {
      if (event.target.matches("[data-stock-filter]")) {
        state.stockFilter = event.target.value;
        withError(loadStock);
      }
    });
    document.addEventListener("DOMContentLoaded", function () {});
    document.getElementById("dialog").addEventListener("click", function (event) {
      if (event.target === event.currentTarget) event.currentTarget.close();
    });
    const DB_LABEL = ${JSON.stringify(DB_NAME)};
    createOperationsPage();
    reportsPage();
    updateClock();
    window.setInterval(updateClock,30000);
    refreshData().catch(function (error) {
      document.getElementById("connection").textContent = "● Database error";
      document.getElementById("connection").style.background = "#fff0ef";
      document.getElementById("connection").style.color = "#bd4d59";
      toast(error.message,true);
    });
  </script>
</body>
</html>`;

if (require.main === module) {
  start();
}

module.exports = {
  APP_HTML,
  buildHealthCentre,
  buildMedicine,
  buildSupplier,
  decorateStock,
  sampleData,
  setupDatabase,
  start,
  stockPipeline,
};
