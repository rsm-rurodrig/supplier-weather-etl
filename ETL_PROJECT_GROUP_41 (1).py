"""
ETL_PROJECT_GROUP_41.py - Final Submission for ETL PROJECT
MEMBERS: Bhavya Goyal, Ruby Rodriguez, Runyu Luo


This script performs the full ETL pipeline:
1. Loads purchase orders from CSVs
2. Calculates PO totals
3. Parses supplier invoice XMLs
4. Loads supplier_case from PostgreSQL
5. Joins ZIPs to NOAA weather stations using geolocation
6. Creates view of ZIP-level daily high temperatures
7. Joins all data into a final enriched view

Note: The only manual step is connecting to NOAA data via Snowflake Marketplace.

"""

import os
import glob
import csv
import psycopg2
import snowflake.connector
from pathlib import Path

# ---------- CONFIGURATION ----------
USER = "bgoyal"
PASSWORD = "Bh@vt@n19112001"
ACCOUNT = "mzyloaw-nxb34526"
WAREHOUSE = "my_first_warehouse"
DATABASE = "testdb"
SCHEMA = "testschema"

CSV_FOLDER = os.path.join(os.getcwd(), "Monthly PO Data")
XML_FILE = os.path.join(os.getcwd(), "Supplier Transactions XML.xml")
SUPPLIER_CASE_CSV = os.path.join(os.getcwd(), "supplier_case.csv")
ZCTA_FILE = os.path.join(os.getcwd(), "ZCTA_UNZIPPED", "2021_Gaz_zcta_national.txt")

# ---------- CONNECT TO SNOWFLAKE ----------
conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA,
)
cs = conn.cursor()

# ---------- SETUP ----------
cs.execute(f"CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE}")
cs.execute(f"USE WAREHOUSE {WAREHOUSE}")
cs.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
cs.execute(f"USE DATABASE {DATABASE}")
cs.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
cs.execute(f"USE SCHEMA {SCHEMA}")

# ---------- CREATE FILE FORMAT ----------
cs.execute("""
CREATE OR REPLACE FILE FORMAT csv_fmt
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL')
""")

# ---------- CREATE STAGES ----------
cs.execute("CREATE OR REPLACE STAGE purchase_stage")
cs.execute("CREATE OR REPLACE STAGE invoice_stage")
cs.execute("CREATE OR REPLACE STAGE stage_supplier_case")
cs.execute("CREATE OR REPLACE STAGE stage_zcta")

# ---------- STEP 1: Load Monthly Purchase CSVs ----------
print(f"Uploading CSVs from: {CSV_FOLDER}")
csv_files = glob.glob(os.path.join(CSV_FOLDER, "*.csv"))
for file_path in sorted(csv_files):
    print(f"Uploading {file_path}...")
    cs.execute(
        f"PUT 'file://{file_path}' @purchase_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    )

# ---------- CREATE PURCHASES TABLE ----------
cs.execute("""
CREATE OR REPLACE TABLE purchases (
    PurchaseOrderID INTEGER,
    SupplierID INTEGER,
    OrderDate DATE,
    DeliveryMethodID INTEGER,
    ContactPersonID INTEGER,
    ExpectedDeliveryDate DATE,
    SupplierReference STRING,
    IsOrderFinalized BOOLEAN,
    Comments STRING,
    InternalComments STRING,
    LastEditedBy STRING,
    LastEditedWhen TIMESTAMP_NTZ,
    PurchaseOrderLineID INTEGER,
    StockItemID INTEGER,
    OrderedOuters INTEGER,
    Description STRING,
    ReceivedOuters INTEGER,
    PackageTypeID INTEGER,
    ExpectedUnitPricePerOuter NUMBER(18,2),
    LastReceiptDate DATE,
    IsOrderLineFinalized NUMBER,
    Right_LastEditedBy STRING,
    Right_LastEditedWhen TIMESTAMP_NTZ
);
""")

# ---------- LOAD PURCHASES WITH TRY_CASTS ----------
cs.execute("""
COPY INTO purchases
FROM (
  SELECT
    TRY_TO_NUMBER($1),
    TRY_TO_NUMBER($2),
    TRY_TO_DATE($3::VARCHAR,'MM/DD/YYYY'),
    TRY_TO_NUMBER($4),
    TRY_TO_NUMBER($5),
    TRY_TO_DATE($6::VARCHAR,'MM/DD/YYYY'),
    $7::STRING,
    IFF(TRY_TO_NUMBER($8)=1,TRUE, IFF(TRY_TO_NUMBER($8)=0,FALSE,NULL)),
    $9::STRING,
    $10::STRING,
    $11::STRING,
    TRY_TO_TIMESTAMP_NTZ($12::VARCHAR),
    TRY_TO_NUMBER($13),
    TRY_TO_NUMBER($14),
    TRY_TO_NUMBER($15),
    $16::STRING,
    TRY_TO_NUMBER($17),
    TRY_TO_NUMBER($18),
    TRY_TO_DECIMAL($19,18,2),
    TRY_TO_DATE($20::VARCHAR,'MM/DD/YYYY'),
    TRY_TO_NUMBER($21),
    $22::STRING,
    TRY_TO_TIMESTAMP_NTZ($23::VARCHAR)
  FROM @purchase_stage (FILE_FORMAT => 'csv_fmt')
)
ON_ERROR='CONTINUE';
""")

print(" purchases table loaded with robust parsing and null handling.")

# ---------- STEP 2: Compute PO totals per PurchaseOrderID ----------
cs.execute("""
CREATE OR REPLACE VIEW purchases_order_totals AS
SELECT
    PurchaseOrderID, SupplierID,
    SUM(ReceivedOuters * ExpectedUnitPricePerOuter) AS POAmount
FROM purchases
GROUP BY PurchaseOrderID, SupplierID
""")

# ---------- STEP 3: Load and Parse XML Invoices ----------
print(f"Uploading XML file: {XML_FILE}")
cs.execute(f"PUT 'file://{XML_FILE}' @invoice_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

cs.execute("""
CREATE OR REPLACE FILE FORMAT FF_XML_SUPPLIER
TYPE = 'XML'
STRIP_OUTER_ELEMENT = TRUE
""")

cs.execute("CREATE OR REPLACE TABLE SUPPLIER_INVOICE_RAW (DATA VARIANT)")

cs.execute("""
COPY INTO SUPPLIER_INVOICE_RAW
FROM @invoice_stage
FILE_FORMAT = (FORMAT_NAME = 'FF_XML_SUPPLIER')
ON_ERROR = 'CONTINUE'
""")
# Shred XML into structured table
cs.execute("""
CREATE OR REPLACE TABLE SUPPLIER_INVOICE AS
SELECT
    TRY_TO_NUMBER(XMLGET(DATA, 'SupplierTransactionID'):"$"::STRING) AS SupplierTransactionID,
    TRY_TO_NUMBER(XMLGET(DATA, 'SupplierID'):"$"::STRING) AS SupplierID,
    TRY_TO_NUMBER(XMLGET(DATA, 'TransactionTypeID'):"$"::STRING) AS TransactionTypeID,
    CASE WHEN REGEXP_LIKE(XMLGET(DATA, 'PurchaseOrderID'):"$"::STRING, '^[0-9]+$')
        THEN TO_NUMBER(XMLGET(DATA, 'PurchaseOrderID'):"$"::STRING) ELSE NULL END AS PurchaseOrderID,
    TRY_TO_NUMBER(XMLGET(DATA, 'PaymentMethodID'):"$"::STRING) AS PaymentMethodID,
    NULLIF(XMLGET(DATA, 'SupplierInvoiceNumber'):"$"::STRING, '') AS SupplierInvoiceNumber,
    TRY_TO_DATE(XMLGET(DATA, 'TransactionDate'):"$"::STRING) AS InvoiceDate,
    TRY_TO_NUMBER(XMLGET(DATA, 'AmountExcludingTax'):"$"::STRING) AS AmountExcludingTax,
    TRY_TO_NUMBER(XMLGET(DATA, 'TaxAmount'):"$"::STRING) AS TaxAmount,
    TRY_TO_NUMBER(XMLGET(DATA, 'TransactionAmount'):"$"::STRING) AS TransactionAmount,
    TRY_TO_NUMBER(XMLGET(DATA, 'OutstandingBalance'):"$"::STRING) AS OutstandingBalance,
    TRY_TO_DATE(XMLGET(DATA, 'FinalizationDate'):"$"::STRING) AS FinalizationDate,
    TRY_TO_NUMBER(XMLGET(DATA, 'IsFinalized'):"$"::STRING) AS IsFinalized,
    TRY_TO_NUMBER(XMLGET(DATA, 'LastEditedBy'):"$"::STRING) AS LastEditedBy,
    TRY_TO_TIMESTAMP_NTZ(XMLGET(DATA, 'LastEditedWhen'):"$"::STRING) AS LastEditedWhen
FROM SUPPLIER_INVOICE_RAW
""")
# ---------- STEP 4 AND 5: Join Invoices to Purchase Orders ----------
cs.execute("""
CREATE OR REPLACE TABLE purchase_orders_and_invoices AS
SELECT
    pot.PurchaseOrderID,
    pot.POAmount,
    pot.SupplierID,
    i.SupplierInvoiceNumber,
    i.AmountExcludingTax,
    (i.AmountExcludingTax - pot.POAmount) AS invoiced_vs_quoted,
    i.InvoiceDate
FROM purchases_order_totals pot
JOIN SUPPLIER_INVOICE i
ON i.PurchaseOrderID = pot.PurchaseOrderID AND i.SupplierID = pot.SupplierID
""")


# ---------- STEP 6: EXPORT SUPPLIER_CASE FROM POSTGRES ----------
def export_supplier_case_to_csv():
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=8765,
        dbname="WestCoastImporters",
        user="jovyan",
        password="postgres",
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM supplier_case;")
    headers = [desc[0] for desc in cursor.description]

    with open(SUPPLIER_CASE_CSV, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in cursor:
            writer.writerow(row)

    cursor.close()
    conn.close()
    print("✅ supplier_case.csv created successfully.")


export_supplier_case_to_csv()

cs.execute("PUT 'file://supplier_case.csv' @stage_supplier_case OVERWRITE=TRUE")

# CREATE supplier_case TABLE
cs.execute("""
CREATE OR REPLACE TABLE supplier_case (
    SupplierID INTEGER,
    SupplierName STRING,
    SupplierCategoryID INTEGER,
    PrimaryContactPersonID INTEGER,
    AlternateContactPersonID INTEGER,
    DeliveryMethodID INTEGER,
    PostalCityID INTEGER,
    SupplierReference STRING,
    BankAccountName STRING,
    BankAccountBranch STRING,
    BankAccountCode INTEGER,
    BankAccountNumber NUMBER,
    BankInternationalCode INTEGER,
    PaymentDays INTEGER,
    InternalComments STRING,
    PhoneNumber STRING,
    FaxNumber STRING,
    WebsiteURL STRING,
    DeliveryAddressLine1 STRING,
    DeliveryAddressLine2 STRING,
    DeliveryPostalCode INTEGER,
    DeliveryLocation STRING,
    PostalAddressLine1 STRING,
    PostalAddressLine2 STRING,
    PostalPostalCode INTEGER,
    LastEditedBy INTEGER,
    ValidFrom STRING,
    ValidTo STRING
)
""")

cs.execute("""
COPY INTO supplier_case
FROM @stage_supplier_case
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE'
""")

# ---------- STEP 7: LOAD ZCTA ----------
cs.execute(f"PUT 'file://{ZCTA_FILE}' @stage_zcta OVERWRITE=TRUE")

cs.execute("""
CREATE OR REPLACE FILE FORMAT TSV_FORMAT
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
FIELD_DELIMITER = '\t'
""")

cs.execute("""
CREATE OR REPLACE TABLE zcta_latlon (
    GEOID STRING,
    LAT FLOAT,
    LON FLOAT
)
""")

cs.execute("""
COPY INTO zcta_latlon (GEOID, LAT, LON)
FROM (
    SELECT $1, $6::FLOAT, $7::FLOAT
    FROM @stage_zcta
)
FILE_FORMAT = (FORMAT_NAME = 'TSV_FORMAT')
ON_ERROR = 'CONTINUE'
""")

# ---------- ZIP-to-STATION USING HAVERSINE ----------
cs.execute("""
CREATE OR REPLACE TABLE supplier_zip_latlon AS
SELECT DISTINCT LPAD(PostalPostalCode::STRING, 5, '0') AS ZIP, z.LAT, z.LON
FROM supplier_case s
JOIN zcta_latlon z ON LPAD(s.PostalPostalCode::STRING, 5, '0') = z.GEOID
""")

cs.execute("""
CREATE OR REPLACE TABLE zip_to_station_ranked AS
SELECT
  sz.ZIP,
  ws.NOAA_WEATHER_STATION_ID AS weather_station_ID,
  ws.LATITUDE,
  ws.LONGITUDE,
  ROW_NUMBER() OVER (
    PARTITION BY sz.ZIP
    ORDER BY
      6371 * 2 * ASIN(SQRT(
        POWER(SIN(RADIANS((sz.LAT - ws.LATITUDE) / 2)), 2) +
        COS(RADIANS(sz.LAT)) * COS(RADIANS(ws.LATITUDE)) *
        POWER(SIN(RADIANS((sz.LON - ws.LONGITUDE) / 2)), 2)
      ))
  ) AS station_rank
FROM supplier_zip_latlon sz
JOIN (
  SELECT DISTINCT NOAA_WEATHER_STATION_ID, LATITUDE, LONGITUDE
  FROM WEATHER__ENVIRONMENT.CYBERSYN.NOAA_WEATHER_STATION_INDEX
  WHERE NOAA_WEATHER_STATION_ID IN (
    SELECT NOAA_WEATHER_STATION_ID
    FROM WEATHER__ENVIRONMENT.CYBERSYN.NOAA_WEATHER_METRICS_TIMESERIES
    WHERE VARIABLE = 'maximum_temperature'
  )
) ws ON TRUE
""")

cs.execute("""
CREATE OR REPLACE TABLE zip_to_station AS
SELECT ZIP, weather_station_ID
FROM zip_to_station_ranked
WHERE station_rank = 1;
""")

cs.execute("""
CREATE OR REPLACE VIEW supplier_zip_code_weather AS
SELECT
  zts.ZIP,
  wt.DATE,
  wt.VALUE AS high_temperature
FROM WEATHER__ENVIRONMENT.CYBERSYN.NOAA_WEATHER_METRICS_TIMESERIES wt
JOIN zip_to_station zts
  ON wt.NOAA_WEATHER_STATION_ID = zts.weather_station_id
WHERE wt.VARIABLE = 'maximum_temperature'
  AND wt.VALUE IS NOT NULL
""")

# ---------- STEP 8: FINAL ENRICHED VIEW ----------
cs.execute("""
CREATE OR REPLACE VIEW enriched_supplier_transactions AS
SELECT
    poi.SupplierID,
    poi.InvoiceDate,
    poi.PurchaseOrderID,
    poi.SupplierInvoiceNumber,
    poi.AmountExcludingTax,
    poi.POAmount,
    poi.invoiced_vs_quoted,
    sc.SupplierName,
    sc.PostalPostalCode,
    weather.DATE AS WeatherDate,
    weather.high_temperature AS HighTemperatureCelsius
FROM purchase_orders_and_invoices poi
JOIN supplier_case sc ON poi.SupplierID = sc.SupplierID
JOIN supplier_zip_code_weather weather
  ON poi.InvoiceDate = weather.DATE
 AND LPAD(sc.PostalPostalCode::STRING, 5, '0') = weather.ZIP
""")

print(" Full pipeline completed successfully.")
cs.execute("SELECT COUNT(*) FROM enriched_supplier_transactions")
final_count = cs.fetchone()[0]
print(f" enriched_supplier_transactions view contains {final_count} rows.")
# enriched_supplier_transactions view contains 1495 rows.

cs.close()
conn.close()
