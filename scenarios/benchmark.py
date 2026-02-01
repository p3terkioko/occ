import argparse
import time
import multiprocessing
import random
import sys
import os
from datetime import datetime

# Add src to path for importing our database client and visualization modules
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))
from client import DBClient

# Import visualization module for automatic graph generation
# This enables us to create publication-ready graphs showing OCC vs 2PL performance
try:
    from visualization import OCC_Visualizer, save_results_for_visualization
    VISUALIZATION_AVAILABLE = True
    print("Visualization module loaded - graphs will be generated automatically")
except ImportError as e:
    VISUALIZATION_AVAILABLE = False
    print(f"Visualization not available: {e}")
    print("   Install matplotlib to enable automatic graph generation: pip install matplotlib seaborn")

def run_client(client_id, mode, num_tx, keys_range, conflict_prob, coordinator_addr, nodes_config, stats_queue):
    """
    Simulates a realistic database client performing concurrent transactions.
    
    This function is the core of our performance analysis - it simulates how real
    applications would interact with our distributed database under different
    concurrency control mechanisms (OCC vs 2PL).
    
    Transaction Pattern Explanation:
    - Each transaction reads 2 random keys and writes to 1 key
    - This simulates typical OLTP workloads (read-modify-write operations)
    - The key range determines conflict probability:
        * Large range (1000 keys): Low conflict - keys rarely overlap between transactions  
        * Small range (10 keys): High conflict - many transactions access same "hot" keys
    
    Args:
        client_id (int): Unique identifier for this client thread
        mode (str): 'OCC' or '2PL' - determines concurrency control mechanism
        num_tx (int): Number of transactions this client should execute
        keys_range (int): Range of keys [0, keys_range) to choose from
        conflict_prob (float): Not used in current implementation (legacy parameter)
        coordinator_addr (tuple): (host, port) of the transaction coordinator
        nodes_config (list): List of (host, port) tuples for data nodes
        stats_queue (Queue): Thread-safe queue to collect performance statistics
    
    Performance Metrics Collected:
    - commits: Number of successfully completed transactions
    - aborts: Number of failed/aborted transactions (due to conflicts or errors)
    - duration: Total time spent executing transactions
    
    Key Insight: In OCC, aborts happen during validation (conflicts detected late)
                 In 2PL, aborts happen during execution (locks unavailable)
    """
    print(f"Starting client {client_id} in {mode} mode - {num_tx} transactions on key range [0, {keys_range})")
    
    client = DBClient(coordinator_addr[0], coordinator_addr[1], nodes_config)
    commits = 0  # Successful transaction counter
    aborts = 0   # Failed transaction counter (conflicts, deadlocks, etc.)
    start_time = time.time()  # Start timing for throughput calculation
    
    for tx_num in range(num_tx):
        try:
            # PHASE 1: BEGIN TRANSACTION
            # In OCC: Get start timestamp from coordinator (establishes "snapshot" point)
            # In 2PL: Generate unique transaction ID for lock management
            client.begin(mode)
            
            # TRANSACTION WORKLOAD: Realistic read-modify-write pattern
            # This simulates common database operations like:
            # - Account balance transfers (read two accounts, update one)
            # - Inventory management (read stock levels, update quantity)
            # - Social media (read posts, update like counts)
            
            # Select keys randomly within the specified range
            # KEY INSIGHT: Smaller key ranges = higher conflict probability
            #              More clients accessing same keys = more contention
            key1 = str(random.randint(0, keys_range - 1))  # Primary key to read
            key2 = str(random.randint(0, keys_range - 1))  # Secondary key to read
            
            # PHASE 2: READ OPERATIONS
            # OCC: Reads current data, adds to ReadSet, no locks acquired
            # 2PL: Must acquire shared/exclusive locks before reading
            val1 = client.read(key1)
            if mode == "2PL" and val1 is None:
                # 2PL lock acquisition failed - another transaction holds the lock
                # In production, we might wait or use deadlock detection
                # Here we abort immediately ("No-Wait" 2PL policy)
                raise Exception(f"2PL locking failed for key {key1} - lock held by another transaction")
                
            val2 = client.read(key2) 
            if mode == "2PL" and val2 is None:
                raise Exception(f"2PL locking failed for key {key2} - lock held by another transaction")

            # BUSINESS LOGIC: Combine the two values (simulates data processing)
            # This represents any computation that depends on read data
            new_val = f"{client_id}-{tx_num}-{(val1 if val1 else '0')}-{(val2 if val2 else '0')}"
            
            # PHASE 3: WRITE OPERATION  
            # OCC: Buffer write locally in WriteSet, don't modify actual data yet
            # 2PL: Must acquire exclusive lock, then write immediately to data store
            write_success = client.write(key1, new_val)
            if not write_success:
                 raise Exception(f"Write operation failed for key {key1}")

            # PHASE 4: COMMIT ATTEMPT
            # OCC: Send ReadSet+WriteSet to coordinator for validation
            #      - Coordinator checks if any committed transaction modified our ReadSet
            #      - If validation passes: apply writes, assign commit timestamp
            #      - If validation fails: abort transaction (conflict detected)
            # 2PL: Release all locks, transaction is already committed
            if client.commit():
                commits += 1
                print(f"   Client {client_id} - Transaction {tx_num} COMMITTED")
            else:
                aborts += 1
                print(f"   Client {client_id} - Transaction {tx_num} ABORTED (validation failed)")
                
        except Exception as e:
            client.abort()
            aborts += 1
            print(f"   Client {client_id} - Transaction {tx_num} EXCEPTION: {str(e)}")
            # Optional: Random backoff to reduce thundering herd effect
            time.sleep(random.uniform(0.01, 0.05))

    duration = time.time() - start_time
    client.close()
    
    # Report client-specific performance
    client_throughput = commits / duration if duration > 0 else 0
    client_abort_rate = (aborts / (commits + aborts)) * 100 if (commits + aborts) > 0 else 0
    print(f"Client {client_id} finished: {commits} commits, {aborts} aborts, "
          f"{client_throughput:.1f} tx/sec, {client_abort_rate:.1f}% abort rate")
    
    stats_queue.put((commits, aborts, duration))

def run_benchmark(mode, clients_count, num_tx_per_client, keys_range):
    """
    Executes a comprehensive benchmark comparing concurrency control mechanisms.
    
    This function orchestrates the entire performance evaluation by:
    1. Launching multiple client processes to simulate concurrent load
    2. Collecting detailed performance statistics
    3. Calculating key metrics (throughput, abort rates, efficiency)
    4. Providing actionable insights for system optimization
    
    The benchmark simulates realistic distributed database workloads where:
    - Multiple clients submit transactions concurrently
    - Transactions access shared data with varying conflict rates
    - System behavior is measured under stress conditions
    
    Args:
        mode (str): 'OCC' or '2PL' - concurrency control mechanism to test
        clients_count (int): Number of concurrent client processes
        num_tx_per_client (int): Transactions each client will execute
        keys_range (int): Key space size (smaller = more conflicts)
    
    Returns:
        dict: Comprehensive performance metrics for analysis
    """
    print(f"\n🚀 === BENCHMARK EXECUTION: {mode} CONCURRENCY CONTROL ===")
    print(f"📊 Configuration:")
    print(f"   • Concurrent Clients: {clients_count}")
    print(f"   • Transactions per Client: {num_tx_per_client}")
    print(f"   • Key Range: [0, {keys_range}) - {'HIGH CONFLICT' if keys_range <= 50 else 'LOW CONFLICT'} scenario")
    print(f"   • Total Expected Transactions: {clients_count * num_tx_per_client}")
    
    # Network configuration for distributed system
    coordinator_addr = ("localhost", 8000)  # Transaction coordinator endpoint
    nodes_config = [("localhost", 8001), ("localhost", 8002)]  # Data node endpoints
    
    print(f"🔗 System Architecture:")
    print(f"   • Coordinator: {coordinator_addr[0]}:{coordinator_addr[1]}")
    print(f"   • Data Nodes: {', '.join([f'{h}:{p}' for h, p in nodes_config])}")
    print(f"   • Data Sharding: Hash-based across {len(nodes_config)} nodes")
    
    # Inter-process communication for collecting statistics
    queue = multiprocessing.Queue()
    procs = []
    
    benchmark_start = time.time()
    
    # Launch concurrent client processes
    # Each process runs independently to simulate real distributed clients
    print(f"\nLaunching {clients_count} concurrent client processes...")
    for i in range(clients_count):
        p = multiprocessing.Process(
            target=run_client, 
            args=(i, mode, num_tx_per_client, keys_range, 0, coordinator_addr, nodes_config, queue)
        )
        p.start()
        procs.append(p)
        
    # Wait for all clients to complete their workloads
    print(f"Waiting for all clients to complete...")
    for i, p in enumerate(procs):
        p.join()  # Blocks until process completes
        print(f"   Client {i} completed")
    
    benchmark_end = time.time()
    total_benchmark_time = benchmark_end - benchmark_start
        
    # STATISTICS COLLECTION AND ANALYSIS
    # Aggregate results from all client processes
    total_commits = 0   # Successfully completed transactions
    total_aborts = 0    # Failed transactions (conflicts, errors, etc.)
    durations = []      # Individual client execution times
    
    # Extract performance data from each client process
    while not queue.empty():
        c, a, d = queue.get()
        total_commits += c
        total_aborts += a
        durations.append(d)
    
    # PERFORMANCE METRICS CALCULATION
    total_attempts = total_commits + total_aborts
    avg_duration = sum(durations) / len(durations) if durations else 0
    
    # Throughput: Successful transactions per second
    # This is the most important metric for database performance
    throughput = total_commits / avg_duration if avg_duration > 0 else 0
    
    # Abort Rate: Percentage of transactions that failed
    # High abort rates indicate excessive contention or system issues
    abort_rate = (total_aborts / total_attempts * 100) if total_attempts > 0 else 0
    
    # System Efficiency: Ratio of useful work to total work
    # Accounts for wasted effort on aborted transactions
    efficiency = (total_commits / total_attempts * 100) if total_attempts > 0 else 0
    
    # RESULTS PRESENTATION
    print(f"\n=== PERFORMANCE RESULTS: {mode} ===")
    print(f"Execution Time: {total_benchmark_time:.2f}s (total benchmark duration)")
    print(f"Successful Commits: {total_commits:,}")
    print(f"Transaction Aborts: {total_aborts:,}")
    print(f"Success Rate: {efficiency:.1f}% ({total_commits}/{total_attempts})")
    print(f"Throughput: {throughput:.1f} successful transactions/second")
    print(f"Abort Rate: {abort_rate:.1f}% ({'HIGH' if abort_rate > 20 else 'LOW'} contention)")
    
    # PERFORMANCE ANALYSIS AND INSIGHTS
    print(f"\n🧠 === ANALYSIS INSIGHTS ===")
    if mode == "OCC":
        if abort_rate < 10:
            print(f"   ✨ EXCELLENT: Low abort rate indicates OCC is highly effective for this workload")
        elif abort_rate < 30:
            print(f"   ⚡ GOOD: Moderate abort rate - OCC provides good performance with some retries")
        else:
            print(f"   ⚠️  POOR: High abort rate suggests excessive conflicts - consider 2PL for this workload")
            
        print(f"   🔍 OCC Behavior: Transactions execute optimistically without locks")
        print(f"      • Read Phase: No blocking, high concurrency")
        print(f"      • Validation: Conflicts detected at commit time")
        print(f"      • Write Phase: Only successful transactions write data")
    else:  # 2PL
        print(f"   🔒 2PL Behavior: Pessimistic locking prevents conflicts")
        print(f"      • Lock Acquisition: Blocking until resources available")
        print(f"      • Deadlock Risk: {'HIGH' if keys_range < 100 else 'LOW'} (depends on key range)")
        print(f"      • Consistency: Strong consistency through exclusive access")
    
    print(f"\n" + "="*60)
    
    # Return structured data for visualization and further analysis
    return {
        'throughput': throughput,
        'abort_rate': abort_rate,
        'commits': total_commits,
        'aborts': total_aborts,
        'efficiency': efficiency,
        'duration': avg_duration,
        'total_time': total_benchmark_time,
        'clients': clients_count,
        'keys_range': keys_range
    }

def run_comprehensive_analysis():
    """
    Executes a complete performance analysis comparing OCC vs 2PL.
    
    This function runs multiple benchmark scenarios to provide comprehensive
    insights into when each concurrency control mechanism performs best.
    It automatically generates visualizations and saves results for analysis.
    """
    print("\n=== COMPREHENSIVE OCC vs 2PL ANALYSIS ===")
    print("This analysis will compare Optimistic and Pessimistic concurrency control")
    print("across different conflict scenarios to determine optimal usage patterns.\n")
    
    # Configuration for comprehensive testing
    clients_count = 5
    tx_per_client = 50
    
    # Store all results for visualization
    all_results = {}
    
    # SCENARIO 1: Low Conflict Environment
    # Large key space means transactions rarely access the same keys
    # This should favor OCC due to minimal conflicts and no locking overhead
    print("SCENARIO 1: LOW CONFLICT ENVIRONMENT (Keys=1000)")
    print("Expected: OCC should outperform 2PL due to minimal conflicts")
    
    all_results['low_conflict'] = {}
    all_results['low_conflict']['OCC'] = run_benchmark('OCC', clients_count, tx_per_client, 1000)
    all_results['low_conflict']['2PL'] = run_benchmark('2PL', clients_count, tx_per_client, 1000)
    
    # SCENARIO 2: High Conflict Environment  
    # Small key space means many transactions access the same "hot" keys
    # This should favor 2PL as it prevents conflicts vs OCC's high abort rate
    print("\nSCENARIO 2: HIGH CONFLICT ENVIRONMENT (Keys=10)")
    print("Expected: 2PL should handle contention better than OCC")
    
    all_results['high_conflict'] = {}
    all_results['high_conflict']['OCC'] = run_benchmark('OCC', clients_count, tx_per_client, 10)
    all_results['high_conflict']['2PL'] = run_benchmark('2PL', clients_count, tx_per_client, 10)
    
    # GENERATE COMPREHENSIVE VISUALIZATIONS
    if VISUALIZATION_AVAILABLE:
        print("\n=== GENERATING PERFORMANCE VISUALIZATIONS ===")
        try:
            # Initialize visualization engine
            visualizer = OCC_Visualizer(output_dir="results")
            
            # Format results for visualization
            viz_data = save_results_for_visualization(all_results)
            
            # Create comprehensive performance comparison
            print("Creating performance comparison charts...")
            visualizer.plot_performance_comparison(viz_data)
            
            # Create transaction lifecycle diagram
            print("Creating OCC transaction lifecycle diagram...")
            visualizer.plot_transaction_lifecycle()
            
            # Generate comprehensive summary report
            print("Creating comprehensive analysis report...")
            visualizer.create_summary_report(viz_data)
            
            print("\nAll visualizations saved to 'results/' directory")
            print("   Charts show performance metrics across different scenarios")
            print("   Graphs highlight when to use OCC vs 2PL")
            print("   Reports provide actionable insights for system optimization")
            
        except Exception as e:
            print(f"Visualization generation failed: {e}")
            print("   Raw performance data is still available above")
    else:
        print("\nAutomatic visualization disabled (matplotlib not available)")
        print("   Install matplotlib and seaborn for automatic graph generation:")
        print("   pip install matplotlib seaborn")
    
    # FINAL RECOMMENDATIONS
    print("\n=== FINAL RECOMMENDATIONS ===")
    
    # Analyze results to provide actionable insights
    try:
        occ_low_throughput = all_results['low_conflict']['OCC']['throughput']
        tpl_low_throughput = all_results['low_conflict']['2PL']['throughput']
        occ_high_abort = all_results['high_conflict']['OCC']['abort_rate']
        tpl_high_abort = all_results['high_conflict']['2PL']['abort_rate']
        
        print(f"LOW CONFLICT: OCC throughput = {occ_low_throughput:.1f} tx/sec, 2PL = {tpl_low_throughput:.1f} tx/sec")
        if occ_low_throughput > tpl_low_throughput:
            improvement = ((occ_low_throughput - tpl_low_throughput) / tpl_low_throughput) * 100
            print(f"   RECOMMENDATION: Use OCC for low-conflict workloads ({improvement:.1f}% better throughput)")
        
        print(f"HIGH CONFLICT: OCC abort rate = {occ_high_abort:.1f}%, 2PL = {tpl_high_abort:.1f}%")
        if occ_high_abort > 30:
            print(f"   RECOMMENDATION: Use 2PL for high-conflict workloads (OCC abort rate too high)")
            
    except Exception as e:
        print(f"   Analysis incomplete: {e}")
    
    print(f"\nAnalysis complete! Check the 'results/' directory for detailed visualizations.")
    return all_results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Benchmark OCC vs 2PL in Distributed Database System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example Usage:
  # Run comprehensive analysis (recommended):
  python benchmark.py --comprehensive
  
  # Test single mode:
  python benchmark.py --mode OCC --clients 5 --keys 100
  
  # High conflict scenario:
  python benchmark.py --mode OCC --keys 10
  
  # Low conflict scenario:  
  python benchmark.py --mode 2PL --keys 1000

Key Parameters:
  --keys: Smaller values = higher conflict rate
  --clients: More clients = higher concurrency
  --tx: More transactions = longer benchmark duration
        """)
    
    parser.add_argument("--comprehensive", action="store_true", 
                       help="Run full OCC vs 2PL analysis with automatic visualization")
    parser.add_argument("--mode", type=str, choices=["OCC", "2PL"], default="OCC",
                       help="Concurrency control mode to test")
    parser.add_argument("--clients", type=int, default=5,
                       help="Number of concurrent client processes")
    parser.add_argument("--tx", type=int, default=20, 
                       help="Number of transactions per client")
    parser.add_argument("--keys", type=int, default=100, 
                       help="Key range size (smaller = higher conflict probability)")
    
    args = parser.parse_args()
    
    if args.comprehensive:
        # Run the complete analysis with visualizations
        run_comprehensive_analysis()
    else:
        # Run single benchmark as requested
        print(f"\nRunning single benchmark: {args.mode} mode")
        result = run_benchmark(args.mode, args.clients, args.tx, args.keys)
        
        # Optionally generate visualization for single run
        if VISUALIZATION_AVAILABLE and input("\nGenerate visualization? (y/n): ").lower().startswith('y'):
            visualizer = OCC_Visualizer()
            # Create a simple results structure for single mode
            single_result = {'single_test': {args.mode: result}}
            viz_data = save_results_for_visualization(single_result)
            
            print("Generating visualization for single benchmark...")
            visualizer.plot_transaction_lifecycle()
            print("Visualization saved to 'results/' directory")
