#!/usr/bin/env python3
"""
Script to decompress Databento .csv.zst files and convert them to CSV format.
This script specifically handles the glbx-mdp3-20250430-20250729.ohlcv-1s.csv.zst file.
"""

import os
import pandas as pd
import zstandard as zstd
from pathlib import Path
from io import StringIO, BytesIO

def decompress_databento_file():
    """
    Decompresses a .csv.zst file from Databento and saves it as a regular CSV file.
    """
    # Define file paths
    base_dir = Path(__file__).parent
    input_file = base_dir / "data" / "raw" / "glbx-mdp3-20250430-20250729.ohlcv-1s.csv.zst"
    output_dir = base_dir / "data" / "processed"
    output_file = output_dir / "glbx-mdp3-20250430-20250729.ohlcv-1s.csv"
    
    # Create the processed directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Created/verified directory: {output_dir}")
    
    # Check if input file exists
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    print(f"File size: {input_file.stat().st_size / (1024*1024):.2f} MB")
    
    try:
        # Initialize zstandard decompressor
        dctx = zstd.ZstdDecompressor()
        
        print("Decompressing file using streaming approach...")
        
        # Use streaming decompression for better memory efficiency
        decompressed_chunks = []
        
        with open(input_file, 'rb') as compressed_file:
            # Use stream reader for better handling of large files
            with dctx.stream_reader(compressed_file) as reader:
                chunk_size = 8192  # 8KB chunks
                total_size = 0
                
                while True:
                    chunk = reader.read(chunk_size)
                    if not chunk:
                        break
                    decompressed_chunks.append(chunk)
                    total_size += len(chunk)
                    
                    # Print progress every 10MB
                    if total_size % (10 * 1024 * 1024) == 0:
                        print(f"Decompressed: {total_size / (1024*1024):.1f} MB")
        
        # Combine all chunks
        decompressed_data = b''.join(decompressed_chunks)
        print(f"Total decompressed size: {len(decompressed_data) / (1024*1024):.2f} MB")
        
        # Check if we actually have data
        if len(decompressed_data) == 0:
            print("⚠️  Warning: Decompressed data is empty!")
            return None
        
        # Convert bytes to string
        try:
            csv_string = decompressed_data.decode('utf-8')
        except UnicodeDecodeError:
            print("⚠️  UTF-8 decode failed, trying latin-1...")
            csv_string = decompressed_data.decode('latin-1')
        
        # Check first few lines of the decompressed data
        lines = csv_string.split('\n')[:10]
        print(f"\nFirst 10 lines of decompressed data:")
        for i, line in enumerate(lines):
            print(f"{i+1:2d}: {line[:100]}{'...' if len(line) > 100 else ''}")
        
        # Create StringIO buffer for pandas
        csv_buffer = StringIO(csv_string)
        
        print("\nLoading data into pandas DataFrame...")
        
        # Try different parsing approaches
        try:
            # First try with default settings
            df = pd.read_csv(csv_buffer, low_memory=False)
        except Exception as e:
            print(f"Default CSV read failed: {e}")
            print("Trying with alternative settings...")
            csv_buffer.seek(0)  # Reset buffer position
            
            # Try with different settings
            df = pd.read_csv(csv_buffer, 
                           low_memory=False,
                           on_bad_lines='skip',
                           encoding='utf-8')
        
        print(f"DataFrame shape: {df.shape}")
        
        if len(df) > 0:
            print(f"Columns: {list(df.columns)}")
            
            # Display the first few rows
            print("\nFirst 5 rows of the DataFrame:")
            print(df.head())
            
            # Display basic info about the DataFrame
            print(f"\nDataFrame info:")
            print(f"- Number of rows: {len(df):,}")
            print(f"- Number of columns: {len(df.columns)}")
            print(f"- Memory usage: {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB")
            
            # Display data types
            print(f"\nData types:")
            print(df.dtypes)
            
            # Check for any missing values
            print(f"\nMissing values per column:")
            print(df.isnull().sum())
            
            # Save the DataFrame as a CSV file
            print(f"\nSaving DataFrame to: {output_file}")
            df.to_csv(output_file, index=False)
            
            # Verify the saved file
            saved_size = output_file.stat().st_size / (1024*1024)
            print(f"Saved file size: {saved_size:.2f} MB")
            
            print("\n✅ Successfully decompressed and saved the file!")
        else:
            print("\n⚠️  Warning: DataFrame is empty after loading!")
            # Still save the file structure for debugging
            df.to_csv(output_file, index=False)
            print(f"Saved empty DataFrame structure to: {output_file}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error during decompression: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    try:
        df = decompress_databento_file()
        if df is not None and len(df) > 0:
            print(f"\n🎉 Process completed successfully!")
            print(f"DataFrame is ready for analysis with {len(df):,} rows and {len(df.columns)} columns.")
        else:
            print(f"\n⚠️  Process completed but DataFrame is empty or None.")
        
    except Exception as e:
        print(f"❌ Script failed: {e}")
        exit(1)
