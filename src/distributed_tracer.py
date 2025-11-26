#!/usr/bin/env python3
"""
Distributed Tracing for Database Operations
Tracks query execution across microservices
"""

import psycopg2
import uuid
import time
import json
from datetime import datetime
from typing import Dict, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class DistributedTracer:
    
    def __init__(self):
        self.conn = None
        self.active_traces = {}
        
    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host='localhost',
                port=5449,
                dbname='tracing_db',
                user='postgres',
                password='postgres'
            )
            self.conn.autocommit = True
            logger.info("Connected to database")
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    def setup(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS traces (
                trace_id VARCHAR(36) PRIMARY KEY,
                service_name VARCHAR(100),
                operation_name VARCHAR(100),
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_ms DECIMAL(10,2),
                status VARCHAR(20),
                metadata JSONB
            );
            
            CREATE TABLE IF NOT EXISTS spans (
                span_id VARCHAR(36) PRIMARY KEY,
                trace_id VARCHAR(36) REFERENCES traces(trace_id),
                parent_span_id VARCHAR(36),
                service_name VARCHAR(100),
                operation_name VARCHAR(100),
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_ms DECIMAL(10,2),
                db_query TEXT,
                rows_affected INT,
                status VARCHAR(20)
            );
            
            CREATE INDEX idx_traces_service ON traces(service_name);
            CREATE INDEX idx_spans_trace ON spans(trace_id);
        """)
        cursor.close()
        logger.info("Tracing tables initialized")
    
    def start_trace(self, service_name: str, operation_name: str) -> str:
        """Start a new distributed trace"""
        trace_id = str(uuid.uuid4())
        
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO traces (trace_id, service_name, operation_name, 
                                start_time, status)
            VALUES (%s, %s, %s, NOW(), 'in_progress')
        """, (trace_id, service_name, operation_name))
        cursor.close()
        
        self.active_traces[trace_id] = {
            'service': service_name,
            'operation': operation_name,
            'start_time': time.time()
        }
        
        logger.info(f"Started trace: {trace_id[:8]}... ({service_name}/{operation_name})")
        return trace_id
    
    def end_trace(self, trace_id: str, status: str = 'success'):
        """End a distributed trace"""
        if trace_id not in self.active_traces:
            logger.warning(f"Trace {trace_id} not found")
            return
        
        trace_info = self.active_traces[trace_id]
        duration = (time.time() - trace_info['start_time']) * 1000
        
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE traces
            SET end_time = NOW(), duration_ms = %s, status = %s
            WHERE trace_id = %s
        """, (duration, status, trace_id))
        cursor.close()
        
        del self.active_traces[trace_id]
        logger.info(f"Ended trace: {trace_id[:8]}... ({duration:.2f}ms)")
    
    def create_span(self, trace_id: str, service_name: str, 
                    operation_name: str, parent_span_id: Optional[str] = None) -> str:
        """Create a span within a trace"""
        span_id = str(uuid.uuid4())
        
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO spans (span_id, trace_id, parent_span_id, 
                               service_name, operation_name, start_time, status)
            VALUES (%s, %s, %s, %s, %s, NOW(), 'in_progress')
        """, (span_id, trace_id, parent_span_id, service_name, operation_name))
        cursor.close()
        
        return span_id
    
    def end_span(self, span_id: str, query: str = None, 
                 rows_affected: int = 0, status: str = 'success'):
        """End a span"""
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE spans
            SET end_time = NOW(),
                duration_ms = EXTRACT(EPOCH FROM (NOW() - start_time)) * 1000,
                db_query = %s,
                rows_affected = %s,
                status = %s
            WHERE span_id = %s
        """, (query, rows_affected, status, span_id))
        cursor.close()
    
    def simulate_microservice_operation(self):
        """Simulate a complex microservice operation with DB calls"""
        
        print("\n" + "=" * 80)
        print("SIMULATING: User Order Creation Flow")
        print("=" * 80)
        
        # Start main trace
        trace_id = self.start_trace('api-gateway', 'create_order')
        
        # Span 1: User Service - Validate User
        print("\n[1] User Service: Validating user...")
        span1 = self.create_span(trace_id, 'user-service', 'validate_user')
        time.sleep(0.05)
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database LIMIT 1")
        cursor.fetchall()
        cursor.close()
        self.end_span(span1, "SELECT * FROM users WHERE user_id = 123", 1)
        print("    Duration: 50ms | Status: SUCCESS")
        
        # Span 2: Inventory Service - Check Stock
        print("\n[2] Inventory Service: Checking stock...")
        span2 = self.create_span(trace_id, 'inventory-service', 'check_stock')
        time.sleep(0.08)
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database LIMIT 1")
        cursor.fetchall()
        cursor.close()
        self.end_span(span2, "SELECT stock FROM products WHERE product_id = 456", 1)
        print("    Duration: 80ms | Status: SUCCESS")
        
        # Span 3: Order Service - Create Order
        print("\n[3] Order Service: Creating order...")
        span3 = self.create_span(trace_id, 'order-service', 'create_order')
        
        # Sub-span: Insert order
        span3a = self.create_span(trace_id, 'order-service', 'insert_order', span3)
        time.sleep(0.03)
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database LIMIT 1")
        cursor.fetchall()
        cursor.close()
        self.end_span(span3a, "INSERT INTO orders VALUES (...)", 1)
        print("    [3a] Insert order: 30ms")
        
        # Sub-span: Update inventory
        span3b = self.create_span(trace_id, 'order-service', 'update_inventory', span3)
        time.sleep(0.04)
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database LIMIT 1")
        cursor.fetchall()
        cursor.close()
        self.end_span(span3b, "UPDATE products SET stock = stock - 1", 1)
        print("    [3b] Update inventory: 40ms")
        
        self.end_span(span3, status='success')
        print("    Total: 70ms | Status: SUCCESS")
        
        # Span 4: Payment Service - Process Payment
        print("\n[4] Payment Service: Processing payment...")
        span4 = self.create_span(trace_id, 'payment-service', 'process_payment')
        time.sleep(0.12)
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database LIMIT 1")
        cursor.fetchall()
        cursor.close()
        self.end_span(span4, "INSERT INTO payments VALUES (...)", 1)
        print("    Duration: 120ms | Status: SUCCESS")
        
        # Span 5: Notification Service - Send Email
        print("\n[5] Notification Service: Sending confirmation...")
        span5 = self.create_span(trace_id, 'notification-service', 'send_email')
        time.sleep(0.06)
        self.end_span(span5, status='success')
        print("    Duration: 60ms | Status: SUCCESS")
        
        # End main trace
        self.end_trace(trace_id, 'success')
        
        print("\n" + "=" * 80)
        return trace_id
    
    def analyze_trace(self, trace_id: str):
        """Analyze a completed trace"""
        
        cursor = self.conn.cursor()
        
        # Get trace info
        cursor.execute("""
            SELECT service_name, operation_name, duration_ms, status
            FROM traces
            WHERE trace_id = %s
        """, (trace_id,))
        
        trace_info = cursor.fetchone()
        
        if not trace_info:
            print("Trace not found")
            cursor.close()
            return
        
        service, operation, duration, status = trace_info
        
        # Get all spans
        cursor.execute("""
            SELECT span_id, parent_span_id, service_name, operation_name,
                   duration_ms, db_query, rows_affected, status
            FROM spans
            WHERE trace_id = %s
            ORDER BY start_time
        """, (trace_id,))
        
        spans = cursor.fetchall()
        cursor.close()
        
        print("\n" + "=" * 80)
        print("TRACE ANALYSIS")
        print("=" * 80)
        print(f"Trace ID: {trace_id}")
        print(f"Operation: {service}/{operation}")
        print(f"Total Duration: {duration:.2f}ms")
        print(f"Status: {status}")
        print(f"\nSpan Breakdown ({len(spans)} spans):")
        
        total_db_time = 0
        
        for span in spans:
            span_id, parent_id, srv, op, dur, query, rows, st = span
            
            indent = "  " if not parent_id else "    "
            
            print(f"\n{indent}Service: {srv}")
            print(f"{indent}Operation: {op}")
            print(f"{indent}Duration: {dur:.2f}ms")
            
            if query:
                print(f"{indent}Query: {query[:60]}...")
                print(f"{indent}Rows: {rows}")
                total_db_time += dur
            
            print(f"{indent}Status: {st}")
        
        db_percentage = (total_db_time / duration * 100) if duration > 0 else 0
        
        print(f"\n" + "=" * 80)
        print("PERFORMANCE METRICS")
        print("=" * 80)
        print(f"Total DB Time: {total_db_time:.2f}ms ({db_percentage:.1f}% of total)")
        print(f"Non-DB Time: {duration - total_db_time:.2f}ms ({100-db_percentage:.1f}% of total)")
        
        # Identify bottlenecks
        slowest_span = max(spans, key=lambda x: x[4])
        print(f"\nSlowest Span:")
        print(f"  Service: {slowest_span[2]}")
        print(f"  Operation: {slowest_span[3]}")
        print(f"  Duration: {slowest_span[4]:.2f}ms")
        
        print("=" * 80)
    
    def get_service_performance(self):
        """Get performance metrics by service"""
        
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                service_name,
                COUNT(*) as span_count,
                AVG(duration_ms) as avg_duration,
                MAX(duration_ms) as max_duration,
                MIN(duration_ms) as min_duration
            FROM spans
            GROUP BY service_name
            ORDER BY avg_duration DESC
        """)
        
        print("\n" + "=" * 80)
        print("SERVICE PERFORMANCE SUMMARY")
        print("=" * 80)
        
        for row in cursor.fetchall():
            service, count, avg, max_dur, min_dur = row
            print(f"\n{service}:")
            print(f"  Span Count: {count}")
            print(f"  Avg Duration: {avg:.2f}ms")
            print(f"  Max Duration: {max_dur:.2f}ms")
            print(f"  Min Duration: {min_dur:.2f}ms")
        
        cursor.close()
        print("=" * 80)
    
    def run_demo(self):
        print("\n" + "=" * 80)
        print("DISTRIBUTED TRACING FOR DATABASE OPERATIONS")
        print("=" * 80)
        
        if not self.connect():
            return
        
        self.setup()
        
        # Simulate operation
        trace_id = self.simulate_microservice_operation()
        
        time.sleep(1)
        
        # Analyze trace
        self.analyze_trace(trace_id)
        
        # Service performance
        self.get_service_performance()
        
        print("\n" + "=" * 80)
        print("Key Features:")
        print("  - End-to-end request tracing")
        print("  - Database query visibility")
        print("  - Performance bottleneck identification")
        print("  - Service dependency mapping")
        print("=" * 80)


def main():
    tracer = DistributedTracer()
    tracer.run_demo()


if __name__ == "__main__":
    main()
