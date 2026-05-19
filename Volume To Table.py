# Databricks notebook source
# DBTITLE 1,Create Delta tables from volume files
import os
import re
from pyspark.sql import functions as F

source_volume = "/Volumes/health/default/dataset"
destination_schema = "health.default"
write_mode = "overwrite"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {destination_schema}")

supported_formats = {
    "csv": "csv"
}

def list_files_recursively(path: str):
    discovered = []
    for item in dbutils.fs.ls(path):
        if item.isDir():
            discovered.extend(list_files_recursively(item.path))
        else:
            discovered.append(item)
    return discovered

def sanitize_table_name(file_name: str) -> str:
    base_name = re.sub(r"\.[^.]+$", "", file_name)
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", base_name).strip("_").lower()
    if not cleaned:
        cleaned = "imported_file"
    if cleaned[0].isdigit():
        cleaned = f"tbl_{cleaned}"
    return cleaned

def read_file(file_path: str, extension: str):
    fmt = supported_formats[extension]

    return (
        spark.read.format(fmt)
        .option("header", True)
        .option("inferSchema", True)
        .load(file_path)
    )

files = list_files_recursively(source_volume)
results = []
used_table_names = set()

for file_info in files:
    file_name = file_info.name.rstrip("/")
    extension = file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""

    if extension not in supported_formats:
        results.append({
            "source_file": file_info.path,
            "table_name": None,
            "status": "skipped",
            "details": f"Unsupported file extension: {extension or 'none'}"
        })
        continue

    table_name = sanitize_table_name(file_name)
    original_table_name = table_name
    suffix = 1
    while table_name in used_table_names:
        suffix += 1
        table_name = f"{original_table_name}_{suffix}"
    used_table_names.add(table_name)

    full_table_name = f"{destination_schema}.{table_name}"

    try:
        df = read_file(file_info.path, extension).withColumn("source_file", F.lit(file_info.path))
        row_count = df.count()
        (
            df.write
            .format("delta")
            .mode(write_mode)
            .option("overwriteSchema", True)
            .saveAsTable(full_table_name)
        )
        results.append({
            "source_file": file_info.path,
            "table_name": full_table_name,
            "status": "created",
            "details": f"Loaded {row_count} rows"
        })
    except Exception as e:
        results.append({
            "source_file": file_info.path,
            "table_name": full_table_name,
            "status": "failed",
            "details": str(e)
        })

results_df = spark.createDataFrame(results)
display(results_df.orderBy("status", "table_name"))
