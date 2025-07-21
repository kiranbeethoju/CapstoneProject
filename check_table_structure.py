#!/usr/bin/env python3
"""
Check Table Structure
Examine the actual structure of tables in Azure SQL Database
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from config.azure_database import (
    test_azure_connection, 
    get_azure_tables, 
    get_table_info, 
    get_azure_db_connection
)

def main():
    """Check table structure"""
    print("🔍 Checking Azure SQL Database Table Structure")
    print("=" * 60)
    
    if not test_azure_connection():
        print("❌ Cannot connect to database")
        return
    
    connection = get_azure_db_connection()
    tables = get_azure_tables()
    
    print(f"📊 Found {len(tables)} tables:")
    print()
    
    for schema, table_name in tables:
        print(f"🗂️ Table: {schema}.{table_name}")
        
        # Get table structure
        columns = get_table_info(schema, table_name)
        
        if columns:
            print("   Columns:")
            for col in columns[:10]:  # Show first 10 columns
                print(f"     - {col[0]} ({col[1]}) - Nullable: {col[2]}")
            
            if len(columns) > 10:
                print(f"     ... and {len(columns) - 10} more columns")
        else:
            print("   No column information available")
        
        print()

if __name__ == "__main__":
    main() 