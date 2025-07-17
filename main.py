#!/usr/bin/env python3
"""
Main entry point for the Document Processor
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime

from config.connections import init_logging, get_s3_client, get_supabase_client, test_connections
from core.document_manager import DocumentManager

def setup_argument_parser():
    """Setup command line argument parser"""
    parser = argparse.ArgumentParser(
        description="S3 Document Processing System",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Test command
    test_parser = subparsers.add_parser('test', help='Test connections and functionality')
    test_parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List documents')
    list_parser.add_argument('--unprocessed', action='store_true', help='Show only unprocessed documents')
    list_parser.add_argument('--processed', action='store_true', help='Show only processed documents')
    list_parser.add_argument('--prefix', default='', help='S3 prefix filter')
    list_parser.add_argument('--export', choices=['json', 'csv'], help='Export format')
    list_parser.add_argument('--output', help='Output file path')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show processing statistics')
    stats_parser.add_argument('--json', action='store_true', help='Output as JSON')
    
    return parser

async def cmd_test(args):
    """Test command implementation"""
    print("Testing Document Processor connections and functionality...")
    
    # Initialize logging
    init_logging()
    
    # Test connections
    results = test_connections()
    
    if args.verbose:
        print(json.dumps(results, indent=2))
    else:
        for service, result in results.items():
            status = "✅" if result['status'] == 'connected' else "❌"
            print(f"{service.upper()}: {status} {result['status']}")
    
    # Test document operations if connections are good
    if all(r['status'] == 'connected' for r in results.values()):
        print("\nTesting document operations...")
        
        s3_client = get_s3_client()
        supabase_client = get_supabase_client()
        manager = DocumentManager(s3_client, supabase_client)
        
        # Quick test
        unprocessed = manager.get_unprocessed_documents()
        print(f"Found {len(unprocessed)} unprocessed documents")
        
        if unprocessed:
            print("Sample unprocessed documents:")
            for doc in unprocessed[:3]:
                print(f"  - {doc.filename} ({doc.file_size:,} bytes)")
    
    return results

async def cmd_list(args):
    """List command implementation"""
    init_logging()
    
    s3_client = get_s3_client()
    supabase_client = get_supabase_client()
    manager = DocumentManager(s3_client, supabase_client)
    
    # Get documents based on filters
    if args.unprocessed:
        documents = manager.get_unprocessed_documents(prefix=args.prefix)
        print(f"Found {len(documents)} unprocessed documents")
    elif args.processed:
        documents = manager.get_processed_documents(prefix=args.prefix)
        print(f"Found {len(documents)} processed documents")
    else:
        # Get all documents from S3
        documents = list(s3_client.list_documents(prefix=args.prefix))
        print(f"Found {len(documents)} total documents")
    
    # Display documents
    if not args.export:
        for i, doc in enumerate(documents[:20]):  # Show first 20
            print(f"{i+1:3d}. {doc.filename}")
            print(f"      Size: {doc.file_size:,} bytes")
            print(f"      Modified: {doc.last_modified}")
            print(f"      S3 Key: {doc.s3_key}")
            if hasattr(doc, 'document_type') and doc.document_type:
                print(f"      Type: {doc.document_type}")
            print()
        
        if len(documents) > 20:
            print(f"... and {len(documents) - 20} more documents")
    
    # Export if requested
    if args.export:
        export_data = manager.export_document_list(documents, format=args.export)
        
        if args.output:
            with open(args.output, 'w') as f:
                if args.export == 'json':
                    json.dump(export_data, f, indent=2, default=str)
                else:
                    f.write(export_data['csv_data'])
            print(f"Exported {len(documents)} documents to {args.output}")
        else:
            if args.export == 'json':
                print(json.dumps(export_data, indent=2, default=str))
            else:
                print(export_data['csv_data'])

async def cmd_stats(args):
    """Stats command implementation"""
    init_logging()
    
    s3_client = get_s3_client()
    supabase_client = get_supabase_client()
    manager = DocumentManager(s3_client, supabase_client)
    
    # Get statistics
    stats = manager.get_statistics()
    
    if args.json:
        print(json.dumps(stats, indent=2, default=str))
    else:
        # Display formatted statistics
        print("Document Processing Statistics")
        print("=" * 50)
        
        s3_stats = stats.get('s3_statistics', {})
        print(f"Total documents in S3: {s3_stats.get('total_in_s3', 0)}")
        
        # File types
        file_types = s3_stats.get('file_types', {})
        if file_types:
            print("\nFile types:")
            for ext, count in sorted(file_types.items()):
                print(f"  .{ext}: {count}")
        
        # Size distribution
        size_dist = s3_stats.get('size_distribution', {})
        if size_dist:
            print("\nSize distribution:")
            for size_range, count in size_dist.items():
                print(f"  {size_range}: {count}")
        
        # Processing progress
        progress = stats.get('processing_progress', {})
        if progress:
            print(f"\nProcessing Progress:")
            print(f"  Total: {progress.get('total_documents', 0)}")
            print(f"  Processed: {progress.get('processed_documents', 0)}")
            print(f"  Unprocessed: {progress.get('unprocessed_documents', 0)}")
            print(f"  Completion: {progress.get('completion_percentage', 0):.1f}%")
        
        # Supabase statistics
        supabase_stats = stats.get('supabase_statistics', {})
        by_type = supabase_stats.get('by_type', {})
        if by_type:
            print(f"\nDocuments by type:")
            for doc_type, count in by_type.items():
                print(f"  {doc_type}: {count}")

async def main():
    """Main function"""
    parser = setup_argument_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'test':
            await cmd_test(args)
        elif args.command == 'list':
            await cmd_list(args)
        elif args.command == 'stats':
            await cmd_stats(args)
        else:
            parser.print_help()
    
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())