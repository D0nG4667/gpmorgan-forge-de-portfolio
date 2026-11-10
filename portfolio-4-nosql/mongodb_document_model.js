// =============================================================================
// GPMorgan Forge: MongoDB 8.0 Document Model, JSON Schema & Aggregations
// Portfolio 4: NoSQL and Advanced Data Models
// Target Engine: MongoDB 8.0+ (mongosh initialization script)
// =============================================================================

const dbName = "gpmorgan_forge_docs";
const forgeDb = db.getSiblingDB(dbName);

// -----------------------------------------------------------------------------
// 1. Collection: kyc_verifications
//    Stores polymorphic multi-jurisdiction KYC payloads (NG BVN/NIN, GB Passport,
//    Corporate CAC filings) with Field-Level Encryption (CSFLE/AEAD) on PII.
// -----------------------------------------------------------------------------
forgeDb.createCollection("kyc_verifications", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: [
        "customer_id",
        "external_ref",
        "jurisdiction",
        "kyc_tier",
        "verification_status",
        "encrypted_pii_fields",
        "document_artifacts",
        "created_at"
      ],
      properties: {
        customer_id: {
          bsonType: "string",
          description: "UUID linking to PostgreSQL customers.customer_id"
        },
        external_ref: { bsonType: "string" },
        jurisdiction: { enum: ["NG", "GB", "US", "EU"] },
        kyc_tier: { enum: ["TIER_1", "TIER_2", "TIER_3"] },
        verification_status: { enum: ["PENDING", "VERIFIED", "REJECTED", "EXPIRED"] },
        encrypted_pii_fields: {
          bsonType: "object",
          required: ["encryption_algorithm", "key_id", "bvn_or_national_id_cipher"],
          properties: {
            encryption_algorithm: { enum: ["AEAD_AES_256_GCM"] },
            key_id: { bsonType: "string" },
            bvn_or_national_id_cipher: { bsonType: "string" },
            tax_id_cipher: { bsonType: "string" }
          }
        },
        document_artifacts: {
          bsonType: "array",
          minItems: 1,
          items: {
            bsonType: "object",
            required: ["doc_type", "sha256_checksum", "liveness_confidence"],
            properties: {
              doc_type: { bsonType: "string" },
              sha256_checksum: { bsonType: "string" },
              liveness_confidence: { bsonType: "double", minimum: 0.0, maximum: 1.0 }
            }
          }
        },
        created_at: { bsonType: "date" }
      }
    }
  }
});

forgeDb.kyc_verifications.createIndex({ customer_id: 1 }, { unique: true, name: "uidx_kyc_customer_id" });
forgeDb.kyc_verifications.createIndex({ jurisdiction: 1, kyc_tier: 1, verification_status: 1 }, { name: "idx_kyc_jurisdiction_tier" });

// -----------------------------------------------------------------------------
// 2. Collection: device_telemetry_events
//    High-velocity mobile & web session telemetry for device-fingerprint AML checks.
// -----------------------------------------------------------------------------
forgeDb.createCollection("device_telemetry_events", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["event_id", "customer_id", "account_id", "device_fingerprint", "ip_address", "risk_signals", "captured_at"],
      properties: {
        event_id: { bsonType: "string" },
        customer_id: { bsonType: "string" },
        account_id: { bsonType: "string" },
        device_fingerprint: { bsonType: "string" },
        ip_address: { bsonType: "string" },
        geo_location: {
          bsonType: "object",
          properties: {
            country: { bsonType: "string" },
            city: { bsonType: "string" },
            coordinates: { bsonType: "array" }
          }
        },
        risk_signals: {
          bsonType: "object",
          required: ["is_emulator", "is_vpn_or_tor", "root_or_jailbreak_detected"],
          properties: {
            is_emulator: { bsonType: "bool" },
            is_vpn_or_tor: { bsonType: "bool" },
            root_or_jailbreak_detected: { bsonType: "bool" }
          }
        },
        captured_at: { bsonType: "date" }
      }
    }
  }
});

forgeDb.device_telemetry_events.createIndex({ device_fingerprint: 1, captured_at: -1 }, { name: "idx_telemetry_device_time" });
forgeDb.device_telemetry_events.createIndex({ captured_at: 1 }, { expireAfterSeconds: 7776000, name: "ttl_telemetry_90_days" });

// -----------------------------------------------------------------------------
// 3. Seed Sample Documents & Analytical Aggregation Pipeline
// -----------------------------------------------------------------------------
forgeDb.kyc_verifications.insertMany([
  {
    customer_id: "10000000-0000-4000-8000-000000000002",
    external_ref: "CUST-NG-00102",
    jurisdiction: "NG",
    kyc_tier: "TIER_3",
    verification_status: "VERIFIED",
    encrypted_pii_fields: {
      encryption_algorithm: "AEAD_AES_256_GCM",
      key_id: "kms-ng-cbn-key-2026-01",
      bvn_or_national_id_cipher: "ENCv2:9f8e7d6c5b4a3928172635445566778899aabbccddeeff",
      tax_id_cipher: "ENCv2:11223344556677889900aabbccddeeff001122334455"
    },
    document_artifacts: [
      {
        doc_type: "CAC_CERTIFICATE_RC109283",
        sha256_checksum: "88f7a91c22d4e61938a0192b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d",
        liveness_confidence: 0.994
      }
    ],
    created_at: new Date("2026-11-01T08:30:00Z")
  },
  {
    customer_id: "10000000-0000-4000-8000-000000000003",
    external_ref: "CUST-NG-00103",
    jurisdiction: "NG",
    kyc_tier: "TIER_3",
    verification_status: "VERIFIED",
    encrypted_pii_fields: {
      encryption_algorithm: "AEAD_AES_256_GCM",
      key_id: "kms-ng-cbn-key-2026-01",
      bvn_or_national_id_cipher: "ENCv2:5566778899aabbccddeeff00112233445566778899aa"
    },
    document_artifacts: [
      {
        doc_type: "NIN_BIOMETRIC_PASSPORT",
        sha256_checksum: "12a34b56c78d90e12f34a56b78c90d12e34f56a78b90c12d34e56f78a90b12c3",
        liveness_confidence: 0.987
      }
    ],
    created_at: new Date("2026-11-01T09:10:00Z")
  }
]);

// Aggregation Pipeline: Detect shared device fingerprints across distinct customers
forgeDb.device_telemetry_events.aggregate([
  {
    $group: {
      _id: "$device_fingerprint",
      distinct_customers: { $addToSet: "$customer_id" },
      distinct_accounts: { $addToSet: "$account_id" },
      event_count: { $sum: 1 },
      vpn_hits: { $sum: { $cond: ["$risk_signals.is_vpn_or_tor", 1, 0] } }
    }
  },
  {
    $project: {
      device_fingerprint: "$_id",
      customer_count: { $size: "$distinct_customers" },
      distinct_customers: 1,
      distinct_accounts: 1,
      event_count: 1,
      vpn_hits: 1
    }
  },
  { $match: { customer_count: { $gt: 1 } } },
  { $sort: { customer_count: -1, event_count: -1 } }
]);
