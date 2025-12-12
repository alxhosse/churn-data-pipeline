"""
Database connection and setup utilities for churn-data-pipeline
"""
import os
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
from typing import Optional


class Database:
    """Database connection and operations handler"""
    
    def __init__(self, 
                 dbname: Optional[str] = None,
                 user: Optional[str] = None,
                 password: Optional[str] = None,
                 host: Optional[str] = None,
                 port: Optional[int] = None,
                 schema: str = 'churn_analytics'):
        """
        Initialize database connection
        
        Args:
            dbname: Database name (defaults to CHURN_DB env var or 'churn')
            user: Database user (defaults to CHURN_DB_USER env var or 'postgres')
            password: Database password (defaults to CHURN_DB_PASS env var)
            host: Database host (defaults to CHURN_DB_HOST env var or 'localhost')
            port: Database port (defaults to 5432)
            schema: Schema name (defaults to 'churn_analytics')
        """
        self.dbname = dbname or os.getenv('CHURN_DB', 'churn')
        self.user = user or os.getenv('CHURN_DB_USER', 'postgres')
        self.password = password or os.getenv('CHURN_DB_PASS', '')
        self.host = host or os.getenv('CHURN_DB_HOST', 'localhost')
        self.port = port or int(os.getenv('CHURN_DB_PORT', '5432'))
        self.schema = schema
        
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection"""
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.cursor = self.conn.cursor()
        return self.conn
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def execute(self, query: str, params: Optional[tuple] = None):
        """Execute a SQL query"""
        if not self.conn:
            self.connect()
        self.cursor.execute(query, params)
        return self.cursor
    
    def commit(self):
        """Commit transaction"""
        if self.conn:
            self.conn.commit()
    
    def create_schema(self):
        """Create the schema if it doesn't exist"""
        query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(self.schema)
        )
        self.execute(query)
        self.commit()
        print(f"Schema '{self.schema}' created or already exists")
    
    def set_search_path(self):
        """Set search path to the schema"""
        query = sql.SQL("SET search_path TO {}").format(
            sql.Identifier(self.schema)
        )
        self.execute(query)
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if exc_type:
            self.conn.rollback()
        else:
            self.commit()
        self.close()

