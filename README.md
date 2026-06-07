# T24 Generic Adapter

## Overview

The T24 Generic Adapter is a metadata-driven, streaming extraction and normalization framework designed to process T24 XML exports and transform them into a standardized, analytics-ready format.

The solution is fully generic and requires no application-specific code. It supports any T24 application such as CUSTOMER, ACCOUNT, FUNDS.TRANSFER, ARRANGEMENT, and custom bank applications by leveraging metadata definitions rather than hardcoded field mappings.

The adapter automatically discovers applications, loads metadata, streams large XML files using constant memory, resolves multi-value and sub-value structures, interprets local reference fields, and produces normalized outputs suitable for data warehousing, analytics, AI, and downstream integration platforms.

---

## System Architecture

![T24 Generic Adapter Architecture](images/T24%20System%20Architecture.png)

---

## Key Capabilities

### Metadata-Driven Processing

* No hardcoded T24 field definitions
* Supports any T24 application without code changes
* Business meaning derived from metadata files
* Automatic field resolution and relationship discovery

### Streaming Architecture

* Processes XML files using incremental parsing
* Constant memory consumption regardless of file size
* Suitable for large-scale T24 exports

### Multi-Value & Sub-Value Support

* Handles standard T24 multi-value fields
* Supports sub-value structures
* Preserves original indexes during normalization

### Local Reference Resolution

* Supports T24 local reference fields (e.g., c64)
* Dynamically resolves local reference positions
* Supports multiple local reference containers

### Customization Support

* Bank-specific custom fields
* User-defined metadata extensions
* Relationship enrichment and lineage tracking

### Multiple Output Formats

* CSV
* JSONL
* Pandas DataFrames
* Custom database targets
* Downstream integration pipelines

---

## Architecture Components

### 1. Validation Layer

Validates package structure, XML integrity, metadata availability, and file consistency before processing begins.

### 2. Discovery Layer

Automatically discovers T24 applications and associated metadata files from the input package structure.

### 3. Metadata Layer

Loads and manages:

* STANDARD.SELECTION
* LOCAL.REF
* CUSTOMIZATION
* RELATIONSHIPS

The metadata registry serves as the central repository for field definitions and relationships.

### 4. Streaming Reader

Reads XML records incrementally using a streaming parser to support very large files without excessive memory consumption.

### 5. Normalization Engine

Transforms raw XML elements into normalized business fields by:

* Resolving field names
* Handling multi-values
* Handling sub-values
* Resolving local references
* Applying custom metadata
* Generating relationship mappings

### 6. Output Layer

Publishes normalized records into configurable target formats for analytics, reporting, integration, and AI workloads.

---

## High-Level Processing Flow

```text
T24 XML Data
      │
      ▼
Validation
      │
      ▼
Application Discovery
      │
      ▼
Metadata Loading
      │
      ▼
Streaming XML Reader
      │
      ▼
Normalization Engine
      │
      ▼
Normalized Output
(CSV / JSONL / DataFrame / Database)
```

---

## Input Structure

```text
t24_input_package/
│
├── data/
│   ├── CUSTOMER.xml
│   ├── ACCOUNT.xml
│   └── FUNDS.TRANSFER.xml
│
├── metadata/
│   ├── STANDARD_SELECTION_CUSTOMER.xml
│   └── STANDARD_SELECTION_ACCOUNT.xml
│
├── local_ref/
│   └── LOCAL_REF_CUSTOMER.xml
│
├── customization/
│   └── CUSTOMIZATION_CUSTOMER.xml
│
├── relationships/
│   └── RELATIONSHIPS_CUSTOMER.xml
│
└── xsd/
    └── CUSTOMER.xsd
```

---

## Supported T24 Concepts

### Standard Fields

Core fields defined through STANDARD.SELECTION metadata.

### Multi-Value Fields

Supports repeated values using T24 multi-value structures.

### Sub-Values

Supports nested value structures within multi-value fields.

### Local References

Supports dynamic field resolution such as:

```text
c64 m=1 → 64.1
c64 m=2 → 64.2
c64 m=3 → 64.3
```

### Relationships

Supports foreign-key style relationships between T24 applications.

### Custom Fields

Supports bank-specific customizations and local extensions.

---

## Design Principles

* Generic by design
* Metadata-first architecture
* No hardcoded application logic
* Constant memory processing
* Extensible and configurable
* Production-ready for enterprise-scale deployments
* Suitable for analytics, AI, data warehousing, and integration use cases

---

## Technology Stack

| Component       | Technology                     |
| --------------- | ------------------------------ |
| Language        | Python 3.9+                    |
| XML Processing  | ElementTree (iterparse)        |
| Data Processing | Pandas (optional)              |
| Architecture    | Metadata-Driven                |
| Memory Model    | Streaming                      |
| Extensibility   | Configuration & Metadata Based |

---

## Typical Use Cases

* T24 Data Migration
* Enterprise Data Warehousing
* Data Lake Ingestion
* Regulatory Reporting
* AML & Fraud Analytics
* Customer 360 Platforms
* AI & Machine Learning Pipelines
* Enterprise Integration Frameworks

---

## References

For detailed implementation guidance, configuration options, metadata structures, and module-level documentation, refer to the T24 Generic Adapter Developer Guide.
