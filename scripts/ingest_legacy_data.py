# scripts/ingest_legacy_data.py
import pandas as pd
import pymssql
import os
from dotenv import load_dotenv
# ─────────────────────────────────────────
# Load Environment Variables
# ─────────────────────────────────────────
load_dotenv()
SQL_SERVER   = os.getenv("SQL_SERVER", "localhost")
SQL_PORT     = int(os.getenv("SQL_PORT", 1433))
SQL_DATABASE = os.getenv("SQL_DATABASE", "ColdChainDB")
SQL_USERNAME = os.getenv("SQL_USERNAME", "SA")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "ColdChain@2024!")
CSV_FILE     = os.getenv("CSV_FILE_PATH")
# ─────────────────────────────────────────
# Step 1: Read CSV File
# ─────────────────────────────────────────
def read_csv(file_path):
    print(f"📂 Reading CSV from: {file_path}")
    df = pd.read_csv(file_path)
    print(f"✅ CSV Loaded!")
    print(f"   Rows:    {len(df)}")
    print(f"   Columns: {list(df.columns)}")
    print(f"\n📊 Preview:")
    print(df.head(3))
    return df
# ─────────────────────────────────────────
# Step 2: Create Database
# ─────────────────────────────────────────
def create_database():
    print(f"\n🗄️  Creating Database: {SQL_DATABASE}")
    conn = pymssql.connect(
        server   = SQL_SERVER,
        port     = SQL_PORT,
        user     = SQL_USERNAME,
        password = SQL_PASSWORD,
        database = "master"
    )
    conn.autocommit(True)
    cursor = conn.cursor()
    cursor.execute(f"""
        IF NOT EXISTS (
            SELECT name FROM sys.databases 
            WHERE name = '{SQL_DATABASE}'
        )
        CREATE DATABASE {SQL_DATABASE}
    """)
    print(f"✅ Database '{SQL_DATABASE}' ready!")
    conn.close()
# ─────────────────────────────────────────
# Step 3: Create Tables
# ─────────────────────────────────────────
def create_tables(conn):
    print(f"\n📋 Creating Tables...")
    cursor = conn.cursor()
    # Drop and recreate ColdChainLogistics
    cursor.execute("""
        IF OBJECT_ID('ColdChainLogistics', 'U') IS NOT NULL
        DROP TABLE ColdChainLogistics
    """)
    cursor.execute("""
        CREATE TABLE ColdChainLogistics (
            timestamp                       DATETIME,
            vehicle_gps_latitude            DECIMAL(18,10),
            vehicle_gps_longitude           DECIMAL(18,10),
            fuel_consumption_rate           DECIMAL(18,6),
            eta_variation_hours             DECIMAL(18,6),
            traffic_congestion_level        DECIMAL(18,6),
            warehouse_inventory_level       DECIMAL(18,6),
            loading_unloading_time          DECIMAL(18,6),
            handling_equipment_availability DECIMAL(18,6),
            order_fulfillment_status        DECIMAL(18,6),
            weather_condition_severity      DECIMAL(18,6),
            port_congestion_level           DECIMAL(18,6),
            shipping_costs                  DECIMAL(18,6),
            supplier_reliability_score      DECIMAL(18,6),
            lead_time_days                  DECIMAL(18,6),
            historical_demand               DECIMAL(18,6),
            iot_temperature                 DECIMAL(18,6),
            cargo_condition_status          DECIMAL(18,6),
            route_risk_level                DECIMAL(18,6),
            customs_clearance_time          DECIMAL(18,6),
            driver_behavior_score           DECIMAL(18,6),
            fatigue_monitoring_score        DECIMAL(18,6),
            disruption_likelihood_score     DECIMAL(18,6),
            delay_probability               DECIMAL(18,6),
            risk_classification             VARCHAR(50),
            delivery_time_deviation         DECIMAL(18,6)
        )
    """)
    print(f"✅ ColdChainLogistics table created!")
    # Drop and recreate AgentAuditLog
    cursor.execute("""
        IF OBJECT_ID('AgentAuditLog', 'U') IS NOT NULL
        DROP TABLE AgentAuditLog
    """)
    cursor.execute("""
        CREATE TABLE AgentAuditLog (
            log_id       INT IDENTITY(1,1) PRIMARY KEY,
            session_id   VARCHAR(100),
            user_query   NVARCHAR(MAX),
            llm_thought  NVARCHAR(MAX),
            tool_called  VARCHAR(100),
            tool_input   NVARCHAR(MAX),
            tool_output  NVARCHAR(MAX),
            final_answer NVARCHAR(MAX),
            created_at   DATETIME DEFAULT GETDATE()
        )
    """)
    print(f"✅ AgentAuditLog table created!")
    conn.commit()
# ─────────────────────────────────────────
# Step 4: Insert Data in Batches
# ─────────────────────────────────────────
def insert_data(conn, df):
    print(f"\n📥 Inserting {len(df)} rows...")
    cursor     = conn.cursor()
    df         = df.where(pd.notnull(df), None)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    batch_size = 500
    total_rows = len(df)
    inserted   = 0
    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i + batch_size]
        for _, row in batch.iterrows():
            cursor.execute("""
                INSERT INTO ColdChainLogistics VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
            """, (
                row['timestamp'],
                row['vehicle_gps_latitude'],
                row['vehicle_gps_longitude'],
                row['fuel_consumption_rate'],
                row['eta_variation_hours'],
                row['traffic_congestion_level'],
                row['warehouse_inventory_level'],
                row['loading_unloading_time'],
                row['handling_equipment_availability'],
                row['order_fulfillment_status'],
                row['weather_condition_severity'],
                row['port_congestion_level'],
                row['shipping_costs'],
                row['supplier_reliability_score'],
                row['lead_time_days'],
                row['historical_demand'],
                row['iot_temperature'],
                row['cargo_condition_status'],
                row['route_risk_level'],
                row['customs_clearance_time'],
                row['driver_behavior_score'],
                row['fatigue_monitoring_score'],
                row['disruption_likelihood_score'],
                row['delay_probability'],
                row['risk_classification'],
                row['delivery_time_deviation']
            ))
        conn.commit()
        inserted += len(batch)
        print(f"   ✅ Inserted {inserted}/{total_rows} rows...")
    print(f"\n🎉 All {total_rows} rows inserted successfully!")
# ─────────────────────────────────────────
# ─────────────────────────────────────────
# Step 5: Verify Data
# ─────────────────────────────────────────
def verify_data(conn):
    print(f"\n🔍 Verifying inserted data...")
    cursor = conn.cursor()
    # Count rows
    cursor.execute("SELECT COUNT(*) FROM ColdChainLogistics")
    count = cursor.fetchone()[0]
    print(f"   Total Rows: {count}")
    # Preview first 5 rows
    cursor.execute("""
        SELECT TOP 5
            timestamp,
            vehicle_gps_latitude,
            vehicle_gps_longitude,
            iot_temperature,
            risk_classification,
            delay_probability
        FROM ColdChainLogistics
    """)
    rows = cursor.fetchall()
    print(f"\n📊 Sample Data:")
    print(f"{'timestamp':<25} {'latitude':<15} {'longitude':<15} {'temperature':<15} {'risk':<15} {'delay_prob':<10}")
    print("-" * 95)
    for row in rows:
        print(f"{str(row[0]):<25} {str(row[1]):<15} {str(row[2]):<15} {str(row[3]):<15} {str(row[4]):<15} {str(row[5]):<10}")
    # Count by risk classification
    cursor.execute("""
        SELECT 
            risk_classification,
            COUNT(*) AS count
        FROM ColdChainLogistics
        GROUP BY risk_classification
        ORDER BY count DESC
    """)
    risk_rows = cursor.fetchall()
    print(f"\n📊 Risk Classification Breakdown:")
    print(f"{'Risk Level':<20} {'Count':<10}")
    print("-" * 30)
    for row in risk_rows:
        print(f"{str(row[0]):<20} {str(row[1]):<10}")
# ─────────────────────────────────────────
# MAIN - Run Everything
# ─────────────────────────────────────────
def main():
    print("=" * 60)
    print("  Cold Chain Logistics - Data Ingestion")
    print("=" * 60)
    # 1. Read CSV
    df = read_csv(CSV_FILE)
    # 2. Create Database
    create_database()
    # 3. Connect to ColdChainDB
    conn = pymssql.connect(
        server   = SQL_SERVER,
        port     = SQL_PORT,
        user     = SQL_USERNAME,
        password = SQL_PASSWORD,
        database = SQL_DATABASE
    )
    print(f"✅ Connected to {SQL_DATABASE}!")
    # 4. Create Tables
    create_tables(conn)
    # 5. Insert Data
    insert_data(conn, df)
    # 6. Verify Data
    verify_data(conn)
    # 7. Close Connection
    conn.close()
    print(f"\n✅ Connection closed!")
    print("=" * 60)
    print("  🎉 Data Ingestion Complete!")
    print("=" * 60)
if __name__ == "__main__":
    main()