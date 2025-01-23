from core.benchmarks.ssb import SSBBenchmark
from core.benchmarks.tpcds import TPCDSBenchmark
from core.benchmarks.tpch import TPCHBenchmark


class BenchmarkFactory:
    def create_benchmark(self, benchmark_type, print_results, clear_cache, interval):
        if benchmark_type == 'tpch':
            return TPCHBenchmark(print_results, clear_cache, interval)
        elif benchmark_type == 'tpcds':
            return TPCDSBenchmark(print_results, clear_cache, interval)
        elif benchmark_type == 'ssb':
            return SSBBenchmark(print_results, clear_cache, interval)
