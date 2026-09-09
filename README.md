<div>
  <h1 align="center">Azure Blob to Snowflake ELT Pipeline</h1>

  <p align="justify">An ELT (Extract, Load, Transform) data pipeline designed for hybrid dataset ingestion (CSV and JSON) from Azure Blob storage into Snowflake, transforming raw staging layers into a dimensional model. The datasets were synthetically generated on Mockaroo.</p>

  <h2>Key Features</h2>

  <ul>
    <li align="justify"><strong>Asymmetric Hybrid Ingestion:</strong> Ingested uneven supplementary attributes from CSV and JSON and handled sparse joins.</li>
    <li align="justify"><strong>Automated Cloud Ingestion:</strong> Connected Snowflake directly to Azure Blob Storage using a SAS token due to Azure student plan limitations (SAS token is obfuscated in the source code for security purposes) and configured pattern-matched Snowpipes.</li>
    <li align="justify"><strong>Deduplication:</strong> Applied SQL window functions for transformation to resolve primary key duplications during ingestion.</li>
    <li align="justify"><strong>Incremental Merging:</strong> Merged new tables' contents into the pre-existing model through Stream object instead of manually reloading the table.</li>
    <li align="justify"><strong>JSON Parsing:</strong> Used Snowflake's VARIANT type and JSON path traversal using (:) operator to extract attributes.</li>
    <li align="justify"><strong>Schema Tiering:</strong> Utilized two (2) schemas, (raw_staging) for data landing and (analytics) for data transformation.</li>
  </ul>
</div>
