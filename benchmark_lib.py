"""
Benchmark Library - Fonctions de conversion et monitoring
"""
import os
import sys
import time
import json
import gc
import threading
import traceback
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import Callable, Optional, Dict, List, Any

import psutil

# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class Config:
    """Configuration du benchmark"""
    # Chemins
    data_dir: str = "data"
    output_dir: str = "benchmark_outputs"
    metrics_dir: str = "benchmark_metrics"
    
    # Timing
    sample_interval: float = 0.5
    cooldown_time: int = 5
    
    # Spark
    spark_driver_memory: str = "8g"
    spark_executor_memory: str = "8g"
    spark_shuffle_partitions: int = 12
    spark_parallelism: int = 12
    spark_master: str = "local[*]"
    hadoop_home: str = "C:\\hadoop"
    
    @classmethod
    def from_env(cls) -> "Config":
        """Charge la configuration depuis .env si present"""
        env_file = Path(".env")
        if env_file.exists():
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, _, value = line.partition("=")
                        os.environ.setdefault(key.strip(), value.strip())
        
        def get_int(key: str, default: int) -> int:
            return int(os.environ.get(key, default))
        
        def get_float(key: str, default: float) -> float:
            return float(os.environ.get(key, default))
        
        return cls(
            data_dir=os.environ.get("DATA_DIR", "data"),
            output_dir=os.environ.get("OUTPUT_DIR", "benchmark_outputs"),
            metrics_dir=os.environ.get("METRICS_DIR", "benchmark_metrics"),
            sample_interval=get_float("SAMPLE_INTERVAL", 0.5),
            cooldown_time=get_int("COOLDOWN_TIME", 5),
            spark_driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", "8g"),
            spark_executor_memory=os.environ.get("SPARK_EXECUTOR_MEMORY", "8g"),
            spark_shuffle_partitions=get_int("SPARK_SHUFFLE_PARTITIONS", 12),
            spark_parallelism=get_int("SPARK_PARALLELISM", 12),
            spark_master=os.environ.get("SPARK_MASTER", "local[*]"),
            hadoop_home=os.environ.get("HADOOP_HOME", "C:\\hadoop"),
        )


# =============================================================================
# PERFORMANCE MONITOR
# =============================================================================

class PerformanceMonitor:
    """Moniteur de performance CPU/RAM en background thread"""
    
    def __init__(self, name: str, interval: float = 0.5, use_system_memory: bool = False):
        self.name = name
        self.interval = interval
        self.metrics: List[Dict] = []
        self.running = False
        self.thread = None
        self.start_time = None
        self.process = psutil.Process()
        self.logical_cpus = psutil.cpu_count(logical=True)
        self.physical_cpus = psutil.cpu_count(logical=False)
        self.use_system_memory = use_system_memory
        self.baseline_memory_mb = 0
    
    def _collect_metrics(self):
        """Collecte les metriques en boucle"""
        while self.running:
            elapsed = time.time() - self.start_time
            system_memory = psutil.virtual_memory()
            system_cpu = psutil.cpu_percent(percpu=False)
            
            if self.use_system_memory:
                # Mode systeme : mesure delta RAM (pour JVM/Spark)
                normalized_cpu = system_cpu
                raw_cpu = system_cpu * self.logical_cpus / 100
                current_system_memory_mb = system_memory.used / (1024 * 1024)
                process_memory_mb = max(0, current_system_memory_mb - self.baseline_memory_mb)
            else:
                memory_info = self.process.memory_info()
                raw_cpu = self.process.cpu_percent()
                
                try:
                    for child in self.process.children(recursive=True):
                        try:
                            raw_cpu += child.cpu_percent()
                            memory_info = type(memory_info)(
                                memory_info.rss + child.memory_info().rss,
                                *memory_info[1:]
                            )
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
                
                normalized_cpu = raw_cpu / self.logical_cpus
                process_memory_mb = memory_info.rss / (1024 * 1024)
            
            self.metrics.append({
                "timestamp": elapsed,
                "cpu_percent": normalized_cpu,
                "memory_mb": process_memory_mb,
                "system_cpu_percent": system_cpu,
                "system_memory_percent": system_memory.percent,
            })
            
            time.sleep(self.interval)
    
    def start(self):
        """Demarre le monitoring"""
        self.metrics = []
        self.running = True
        self.start_time = time.time()
        self.process.cpu_percent()
        if self.use_system_memory:
            psutil.cpu_percent()
            self.baseline_memory_mb = psutil.virtual_memory().used / (1024 * 1024)
            time.sleep(0.1)
        self.thread = threading.Thread(target=self._collect_metrics, daemon=True)
        self.thread.start()
    
    def stop(self) -> Dict:
        """Arrete le monitoring et retourne les resultats"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=1)
        
        duration = time.time() - self.start_time
        
        if self.metrics:
            cpu_values = [m["cpu_percent"] for m in self.metrics]
            mem_values = [m["memory_mb"] for m in self.metrics]
            
            stats = {
                "name": self.name,
                "duration_seconds": duration,
                "samples_count": len(self.metrics),
                "cpu_avg": sum(cpu_values) / len(cpu_values),
                "cpu_max": max(cpu_values),
                "memory_avg_mb": sum(mem_values) / len(mem_values),
                "memory_max_mb": max(mem_values),
            }
        else:
            stats = {
                "name": self.name,
                "duration_seconds": duration,
                "samples_count": 0,
            }
        
        return {"stats": stats, "timeseries": self.metrics}


# =============================================================================
# BENCHMARK RUNNER
# =============================================================================

def run_benchmark(name: str, func: Callable, config: Config = None, 
                  use_system_memory: bool = False, save_metrics: bool = True) -> Dict:
    """Execute un benchmark avec monitoring"""
    if config is None:
        config = Config()
    
    print(f"\n[START] {name}")
    
    monitor = PerformanceMonitor(name, interval=config.sample_interval, use_system_memory=use_system_memory)
    monitor.start()
    
    extra_data = {}
    try:
        result = func()
        if isinstance(result, dict):
            extra_data = result
        success = True
        error_msg = None
    except Exception as e:
        success = False
        error_msg = str(e)
        print(f"[ERROR] {name}: {error_msg}")
        traceback.print_exc()
    
    results = monitor.stop()
    results["success"] = success
    results["error"] = error_msg
    results.update(extra_data)
    
    # Sauvegarder les metriques
    if save_metrics:
        os.makedirs(config.metrics_dir, exist_ok=True)
        metrics_file = os.path.join(config.metrics_dir, f"{name.lower()}_metrics.json")
        with open(metrics_file, "w") as f:
            json.dump(results, f, indent=2)
    
    # Afficher le resume
    if success:
        stats = results["stats"]
        print(f"[OK] {name}: {stats['duration_seconds']:.2f}s")
        print(f"     CPU: {stats['cpu_avg']:.1f}% avg | {stats['cpu_max']:.1f}% max")
        print(f"     RAM: {stats['memory_avg_mb']/1024:.1f} GB avg | {stats['memory_max_mb']/1024:.1f} GB max")
    
    return results


def cleanup_memory(cooldown: int = 0, next_name: str = None):
    """Nettoie la RAM et attend"""
    print("\n[CLEANUP] Nettoyage memoire...", end=" ", flush=True)
    gc.collect()
    gc.collect()
    
    mem_before = psutil.virtual_memory().percent
    time.sleep(1)
    gc.collect()
    mem_after = psutil.virtual_memory().percent
    print(f"RAM: {mem_before:.1f}% -> {mem_after:.1f}%")
    
    if cooldown > 0 and next_name:
        print(f"[WAIT] Cooldown {cooldown}s avant {next_name}...", end=" ", flush=True)
        for i in range(cooldown, 0, -1):
            print(f"{i}", end=" ", flush=True)
            time.sleep(1)
        print("OK")


# =============================================================================
# CONVERSION FUNCTIONS
# =============================================================================

def convert_pandas(input_file: str, output_dir: str) -> Dict:
    """Conversion CSV -> Parquet avec Pandas"""
    import pandas as pd
    df = pd.read_csv(input_file)
    df.to_parquet(f"{output_dir}/pandas.parquet")
    return {"rows": len(df)}


def convert_polars(input_file: str, output_dir: str) -> Dict:
    """Conversion CSV -> Parquet avec Polars (lazy streaming)"""
    import polars as pl
    q = pl.scan_csv(input_file)
    q.sink_parquet(f"{output_dir}/polars.parquet", compression="snappy")
    return {}


def convert_duckdb(input_file: str, output_dir: str) -> Dict:
    """Conversion CSV -> Parquet avec DuckDB"""
    import duckdb
    duckdb.sql(f"""
        COPY (
            SELECT * FROM read_csv('{input_file}', 
                header=true, 
                quote='"',
                ignore_errors=true
            )
        ) TO '{output_dir}/duckdb.parquet' (FORMAT 'PARQUET')
    """)
    return {}


def convert_pyspark(input_file: str, output_dir: str, config: Config = None) -> Dict:
    """Conversion CSV -> Parquet avec PySpark"""
    from pyspark.sql import SparkSession
    
    if config is None:
        config = Config()
    
    # Configuration Windows
    if config.hadoop_home:
        os.environ["HADOOP_HOME"] = config.hadoop_home
        os.environ["PATH"] = os.environ.get("PATH", "") + f";{config.hadoop_home}\\bin"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    timings = {"session_start": time.time()}
    
    spark = SparkSession.builder \
        .appName("Benchmark") \
        .master(config.spark_master) \
        .config("spark.driver.memory", config.spark_driver_memory) \
        .config("spark.executor.memory", config.spark_executor_memory) \
        .config("spark.sql.shuffle.partitions", str(config.spark_shuffle_partitions)) \
        .config("spark.default.parallelism", str(config.spark_parallelism)) \
        .config("spark.sql.warehouse.dir", os.path.abspath(output_dir)) \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    timings["session_ready"] = time.time()
    spark.sparkContext.setLogLevel("ERROR")
    
    df = spark.read.csv(input_file, header=True, inferSchema=False)
    
    timings["write_start"] = time.time()
    df.write.mode("overwrite").parquet(f"{output_dir}/pyspark_out")
    timings["write_end"] = time.time()
    
    spark.stop()
    
    session_time = timings["session_ready"] - timings["session_start"]
    write_time = timings["write_end"] - timings["write_start"]
    print(f"     Spark session: {session_time:.2f}s | Write: {write_time:.2f}s")
    
    return {
        "pyspark_details": {
            "session_startup_seconds": session_time,
            "write_seconds": write_time,
        }
    }


# =============================================================================
# UTILS
# =============================================================================

def find_csv_file(data_dir: str = "data", path: str = None) -> str:
    """Trouve le fichier CSV le plus gros dans data_dir"""
    if path and os.path.isfile(path):
        return path
    
    data_path = Path(data_dir)
    if not data_path.exists():
        raise FileNotFoundError(f"Dossier '{data_dir}/' non trouve")
    
    csv_files = list(data_path.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"Aucun fichier CSV dans '{data_dir}/'")
    
    largest = max(csv_files, key=lambda f: f.stat().st_size)
    return str(largest)


def get_system_info() -> Dict:
    """Retourne les infos systeme"""
    return {
        "cpu_physical": psutil.cpu_count(logical=False),
        "cpu_logical": psutil.cpu_count(logical=True),
        "ram_gb": psutil.virtual_memory().total / (1024**3),
    }


def save_summary(results: Dict, config: Config, input_file: str):
    """Sauvegarde le resume global"""
    summary = {
        "timestamp": datetime.now().isoformat(),
        "input_file": input_file,
        "file_size_gb": os.path.getsize(input_file) / (1024**3) if os.path.exists(input_file) else 0,
        "system": get_system_info(),
        "results": {
            name: {
                "success": r["success"],
                "duration": r["stats"]["duration_seconds"] if r["success"] else None,
                "cpu_avg": r["stats"].get("cpu_avg"),
                "cpu_max": r["stats"].get("cpu_max"),
                "memory_max_mb": r["stats"].get("memory_max_mb"),
            }
            for name, r in results.items()
        }
    }
    
    os.makedirs(config.metrics_dir, exist_ok=True)
    summary_file = os.path.join(config.metrics_dir, "benchmark_summary.json")
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)
    
    return summary
