# ADR 0006: Zero-Trust Database Security, RBAC, ABAC, RLS & NDPA 2023 Privacy Engineering

- **Status**: Accepted
- **Date**: 2026-11-08
- **Deciders**: Information Security & Data Protection Officer (DPO) Team
- **Related Deliverables**: [`portfolio-5-distributed-cloud/security_rbac_rls_privacy.sql`](../../portfolio-5-distributed-cloud/security_rbac_rls_privacy.sql), [`src/gpmorgan_forge/distributed_privacy.py`](../../src/gpmorgan_forge/distributed_privacy.py)

---

## Context

Financial institutions operating in Nigeria and internationally are governed by strict statutory frameworks:

- **Nigeria Data Protection Act (NDPA 2023)** (enforced by the NDPC) and **EU GDPR**: Mandate explicit opt-in consent, purpose limitation, data minimisation, technical encryption/pseudonymisation, DPO auditability, and the Data Subject **Right to Erasure**.
- **Central Bank of Nigeria (CBN) AML/CFT Regulations**: Mandate retaining financial transaction ledgers for a minimum of **7 years**.
- **PCI-DSS v4.0**: Prohibits storing cleartext Primary Account Numbers (PANs) and requires dynamic masking (`XXXX-XXXX-XXXX-1234`) for analysts and operational staff.

Relying solely on application-layer `WHERE` clauses for access control is vulnerable to SQL injection, ORM misconfiguration, or insider privilege abuse.

---

## Decision

Embed **Privacy by Design** and **Zero-Trust Defense-in-Depth** directly inside PostgreSQL 18 ([`security_rbac_rls_privacy.sql`](../../portfolio-5-distributed-cloud/security_rbac_rls_privacy.sql)) and the Python security/privacy layer (`AccessControlEngine` and `DataPrivacyEngine` in [`distributed_privacy.py`](../../src/gpmorgan_forge/distributed_privacy.py)):

1. **Database-Enforced RBAC (`NOLOGIN` Group Roles)**: Partition privileges across five least-privilege roles: `forge_admin`, `forge_core_service`, `forge_compliance_dpo`, `forge_analyst`, and `forge_customer`. Revoke all default `PUBLIC` table permissions.
2. **Row-Level Security (RLS) & Attribute-Based Access Control (ABAC)**: Enable `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` and `FORCE ROW LEVEL SECURITY` on `customers` and `accounts`:
   - `pol_customers_customer_isolation` & `pol_accounts_customer_isolation`: Restricts `forge_customer` to rows where `customer_id = NULLIF(current_setting('app.current_customer_id', TRUE), '')::uuid`.
   - `pol_customers_dpo_jurisdiction`: Restricts `forge_compliance_dpo` via ABAC attribute `country_code = NULLIF(current_setting('app.actor_country', TRUE), '')`.
   - `pol_customers_core_all` & `pol_accounts_core_and_dpo_all`: Grants operational access to `forge_core_service` (and `forge_compliance_dpo` on accounts).
3. **Dynamic Data Masking Views**: Analysts (`forge_analyst`) query `v_masked_customers` and `v_masked_payment_cards` rather than base tables, masking names (`Ch***`), emails (`ch***@example.ng`), phones (`+234******3303`), BVN hashes (`REDACTED_SHA256`), and card PANs (`XXXX-XXXX-XXXX-4821`).
4. **Conflict-Free Right-to-Erasure (`anonymise_customer_pii`)**: When a customer invokes their NDPA 2023 / GDPR Right to Erasure, `anonymise_customer_pii()` and `DataPrivacyEngine.anonymise_customer_record()` irreversibly redact PII fields (`REDACTED_<hash>`), revoke active `consent_records`, and append an immutable audit log while preserving `customer_id`, `accounts`, and `ledger_entries` required by CBN 7-year retention laws.
5. **Differential Privacy & Zero-Knowledge Proofs (ZKPs)**: Aggregate analytics inject calibrated Laplace noise $\text{Lap}(\Delta f / \epsilon)$ (`apply_differential_privacy_laplace`), and KYC tier/age claims are verified via cryptographic SHA-256 commitments (`generate_and_verify_zkp_tier_claim`) without disclosing raw birthdates or BVN numbers.

---

## Consequences

- **Positive**: Eliminates multi-tenant data leakage even if an application query omits a filter; resolves the legal tension between NDPA 2023 Right to Erasure and CBN 7-year ledger retention; satisfies PCI-DSS v4.0 PAN masking and modern cryptographic privacy standards.
- **Negative / Trade-offs**: Connection pools must set session context variables (`SET LOCAL app.current_customer_id = ...`) at the start of each unit of work.
