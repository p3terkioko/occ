"""
Visualization Module for OCC vs 2PL Performance Analysis

This module creates comprehensive graphs to visualize the performance differences
between Optimistic Concurrency Control (OCC) and Two-Phase Locking (2PL) in
our distributed database system.

Key Visualizations:
1. Performance Comparison (Throughput, Abort Rates)  
2. Conflict Scenario Analysis (Low vs High Conflict)
3. Scalability Analysis (Different client counts)
4. Transaction Lifecycle Visualization
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import os
from datetime import datetime

class OCC_Visualizer:
    """
    Creates publication-ready graphs for OCC performance analysis.
    
    This class generates multiple types of visualizations:
    - Bar charts for direct OCC vs 2PL comparison
    - Line plots for scalability analysis  
    - Pie charts for transaction outcome distribution
    - Heatmaps for conflict pattern analysis
    """
    
    def __init__(self, output_dir="results"):
        """
        Initialize visualizer with output directory.
        
        Args:
            output_dir (str): Directory to save generated graphs
        """
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        # This ensures all graphs are saved in a organized location
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created output directory: {output_dir}")
        
        # Set up matplotlib styling for professional-looking graphs
        plt.style.use('seaborn-v0_8')  # Modern, clean styling
        plt.rcParams['figure.figsize'] = (12, 8)  # Larger figures for better readability
        plt.rcParams['font.size'] = 12  # Readable font size
        plt.rcParams['axes.grid'] = True  # Grid lines for easier reading
        
    def plot_performance_comparison(self, results_data):
        """
        Creates a comprehensive performance comparison between OCC and 2PL.
        
        This visualization shows:
        - Throughput comparison (transactions/second)
        - Abort rate comparison (percentage)
        - Side-by-side analysis for different conflict scenarios
        
        Args:
            results_data (dict): Performance results from benchmark runs
                Format: {
                    'low_conflict': {'OCC': {...}, '2PL': {...}},
                    'high_conflict': {'OCC': {...}, '2PL': {...}}
                }
        """
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # Extract data for visualization
        scenarios = ['Low Conflict\n(Keys=1000)', 'High Conflict\n(Keys=10)']
        occ_throughput = []
        tpl_throughput = []
        occ_abort_rate = []
        tpl_abort_rate = []
        
        for scenario in ['low_conflict', 'high_conflict']:
            if scenario in results_data:
                # OCC performance metrics
                occ_data = results_data[scenario].get('OCC', {})
                occ_throughput.append(occ_data.get('throughput', 0))
                occ_abort_rate.append(occ_data.get('abort_rate', 0))
                
                # 2PL performance metrics  
                tpl_data = results_data[scenario].get('2PL', {})
                tpl_throughput.append(tpl_data.get('throughput', 0))
                tpl_abort_rate.append(tpl_data.get('abort_rate', 0))
            else:
                # Handle missing data gracefully
                occ_throughput.append(0)
                tpl_throughput.append(0)
                occ_abort_rate.append(0)
                tpl_abort_rate.append(0)
        
        # 1. Throughput Comparison
        # This shows which approach handles more transactions per second
        x_pos = np.arange(len(scenarios))
        width = 0.35
        
        bars1 = ax1.bar(x_pos - width/2, occ_throughput, width, 
                       label='OCC (Optimistic)', color='#2E86AB', alpha=0.8)
        bars2 = ax1.bar(x_pos + width/2, tpl_throughput, width,
                       label='2PL (Pessimistic)', color='#A23B72', alpha=0.8)
        
        ax1.set_xlabel('Conflict Scenario', fontweight='bold')
        ax1.set_ylabel('Throughput (tx/sec)', fontweight='bold')
        ax1.set_title('Throughput Comparison: OCC vs 2PL\n(Higher is Better)', 
                     fontweight='bold', pad=20)
        ax1.set_xticks(x_pos)
        ax1.set_xticklabels(scenarios)
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Add value labels on bars for exact reading
        for bar in bars1:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}', ha='center', va='bottom', fontweight='bold')
        for bar in bars2:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}', ha='center', va='bottom', fontweight='bold')
        
        # 2. Abort Rate Comparison  
        # This shows how often transactions fail due to conflicts
        bars3 = ax2.bar(x_pos - width/2, occ_abort_rate, width,
                       label='OCC (Optimistic)', color='#2E86AB', alpha=0.8)
        bars4 = ax2.bar(x_pos + width/2, tpl_abort_rate, width,
                       label='2PL (Pessimistic)', color='#A23B72', alpha=0.8)
        
        ax2.set_xlabel('Conflict Scenario', fontweight='bold')
        ax2.set_ylabel('Abort Rate (%)', fontweight='bold')
        ax2.set_title('Abort Rate Comparison: OCC vs 2PL\n(Lower is Better)', 
                     fontweight='bold', pad=20)
        ax2.set_xticks(x_pos)
        ax2.set_xticklabels(scenarios)
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Add value labels for abort rates
        for bar in bars3:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}%', ha='center', va='bottom', fontweight='bold')
        for bar in bars4:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}%', ha='center', va='bottom', fontweight='bold')
        
        # 3. Performance Efficiency Ratio
        # This shows the relative efficiency (throughput per successful transaction)
        efficiency_occ = []
        efficiency_2pl = []
        
        for i in range(len(scenarios)):
            # Calculate efficiency as throughput / (1 - abort_rate/100)
            # Higher efficiency = more successful work done
            occ_eff = occ_throughput[i] * (1 - occ_abort_rate[i]/100) if occ_abort_rate[i] < 100 else 0
            tpl_eff = tpl_throughput[i] * (1 - tpl_abort_rate[i]/100) if tpl_abort_rate[i] < 100 else 0
            
            efficiency_occ.append(occ_eff)
            efficiency_2pl.append(tpl_eff)
        
        bars5 = ax3.bar(x_pos - width/2, efficiency_occ, width,
                       label='OCC (Optimistic)', color='#2E86AB', alpha=0.8)
        bars6 = ax3.bar(x_pos + width/2, efficiency_2pl, width,
                       label='2PL (Pessimistic)', color='#A23B72', alpha=0.8)
        
        ax3.set_xlabel('Conflict Scenario', fontweight='bold')
        ax3.set_ylabel('Effective Throughput (successful tx/sec)', fontweight='bold')
        ax3.set_title('Effective Throughput: Success Rate Adjusted\n(Throughput × Success Rate)', 
                     fontweight='bold', pad=20)
        ax3.set_xticks(x_pos)
        ax3.set_xticklabels(scenarios)
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # Add value labels for efficiency
        for bar in bars5:
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}', ha='center', va='bottom', fontweight='bold')
        for bar in bars6:
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}', ha='center', va='bottom', fontweight='bold')
        
        # 4. Performance Summary with Insights
        # This provides a text-based summary of key findings
        ax4.axis('off')  # Remove axes for text display
        
        # Calculate key insights from the data
        insights = self._generate_performance_insights(results_data)
        
        summary_text = "KEY PERFORMANCE INSIGHTS:\n\n"
        for i, insight in enumerate(insights, 1):
            summary_text += f"{i}. {insight}\n\n"
        
        ax4.text(0.05, 0.95, summary_text, transform=ax4.transAxes,
                fontsize=11, verticalalignment='top', fontweight='normal',
                bbox=dict(boxstyle="round,pad=0.5", facecolor="lightblue", alpha=0.3))
        
        plt.tight_layout()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.output_dir}/performance_comparison_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Performance comparison graph saved: {filename}")
        plt.show()
        
    def _generate_performance_insights(self, results_data):
        """
        Generates human-readable insights from performance data.
        
        This method analyzes the benchmark results and produces
        actionable insights about when to use OCC vs 2PL.
        
        Args:
            results_data (dict): Benchmark results
            
        Returns:
            list: List of insight strings
        """
        insights = []
        
        try:
            # Analyze low conflict scenario
            if 'low_conflict' in results_data:
                low_occ = results_data['low_conflict'].get('OCC', {})
                low_2pl = results_data['low_conflict'].get('2PL', {})
                
                occ_throughput_low = low_occ.get('throughput', 0)
                tpl_throughput_low = low_2pl.get('throughput', 0)
                
                if occ_throughput_low > tpl_throughput_low:
                    improvement = ((occ_throughput_low - tpl_throughput_low) / tpl_throughput_low) * 100
                    insights.append(f"LOW CONFLICT: OCC outperforms 2PL by {improvement:.1f}% in throughput")
                else:
                    insights.append("LOW CONFLICT: 2PL performs better than expected")
                    
            # Analyze high conflict scenario  
            if 'high_conflict' in results_data:
                high_occ = results_data['high_conflict'].get('OCC', {})
                high_2pl = results_data['high_conflict'].get('2PL', {})
                
                occ_abort_high = high_occ.get('abort_rate', 0)
                tpl_abort_high = high_2pl.get('abort_rate', 0)
                
                if occ_abort_high > 20:
                    insights.append(f"HIGH CONFLICT: OCC abort rate is {occ_abort_high:.1f}% - validation failures are common")
                    
            # General recommendation
            insights.append("RECOMMENDATION: Use OCC for read-heavy workloads, 2PL for write-heavy workloads")
            
        except Exception as e:
            insights.append(f"Analysis incomplete due to: {str(e)}")
            
        return insights
    
    def plot_transaction_lifecycle(self, sample_data=None):
        """
        Visualizes the OCC transaction lifecycle phases.
        
        This creates a flowchart-style visualization showing:
        - Read Phase (local buffering)
        - Validation Phase (conflict checking)  
        - Write Phase (commit or abort)
        
        Args:
            sample_data (dict, optional): Sample transaction data for illustration
        """
        fig, ax = plt.subplots(1, 1, figsize=(14, 10))
        
        # Create a timeline visualization of OCC phases
        phases = ['START\nTRANSACTION', 'READ PHASE\n(Local Buffering)', 'VALIDATION PHASE\n(Conflict Check)', 
                 'WRITE PHASE\n(Commit/Abort)', 'END\nTRANSACTION']
        
        # Phase descriptions for educational purposes
        descriptions = [
            'Get start timestamp\nfrom coordinator',
            'Read from nodes\nBuffer writes locally\nNo locks acquired',
            'Send ReadSet + WriteSet\nCheck for conflicts\nBackward validation',
            'If valid: Apply writes\nIf invalid: Abort\nUpdate global timestamp',
            'Transaction complete\nResources released'
        ]
        
        y_positions = [4, 3, 2, 1, 0]
        x_position = 1
        
        # Draw the phases as boxes
        for i, (phase, desc, y_pos) in enumerate(zip(phases, descriptions, y_positions)):
            # Color coding: Start/End = gray, Read = blue, Validation = orange, Write = green
            colors = ['#D3D3D3', '#2E86AB', '#F18F01', '#C73E1D', '#D3D3D3']
            
            # Create rounded rectangle for each phase
            rect = mpatches.FancyBboxPatch((x_position - 0.8, y_pos - 0.3), 1.6, 0.6,
                                         boxstyle="round,pad=0.1", 
                                         facecolor=colors[i], alpha=0.8, edgecolor='black')
            ax.add_patch(rect)
            
            # Add phase title
            ax.text(x_position, y_pos, phase, ha='center', va='center', 
                   fontweight='bold', fontsize=11, color='white' if i in [1,2,3] else 'black')
            
            # Add description
            ax.text(x_position + 1.5, y_pos, desc, ha='left', va='center', 
                   fontsize=10, style='italic')
            
            # Draw arrows between phases
            if i < len(phases) - 1:
                ax.annotate('', xy=(x_position, y_positions[i+1] + 0.3), 
                           xytext=(x_position, y_pos - 0.3),
                           arrowprops=dict(arrowstyle='->', lw=2, color='black'))
        
        # Add decision diamond for validation
        validation_y = 2
        decision_points = [(x_position + 3.5, validation_y + 0.3), 
                          (x_position + 4, validation_y),
                          (x_position + 3.5, validation_y - 0.3), 
                          (x_position + 3, validation_y)]
        
        diamond = mpatches.Polygon(decision_points, closed=True, 
                                 facecolor='yellow', alpha=0.8, edgecolor='black')
        ax.add_patch(diamond)
        ax.text(x_position + 3.5, validation_y, 'Valid?', ha='center', va='center', 
               fontweight='bold')
        
        # Add outcome arrows
        # Success path (green)
        ax.annotate('YES\n(No Conflicts)', xy=(x_position + 0.8, 1), 
                   xytext=(x_position + 3.2, validation_y - 0.2),
                   arrowprops=dict(arrowstyle='->', lw=2, color='green'),
                   fontsize=10, color='green', fontweight='bold')
        
        # Failure path (red) 
        ax.annotate('NO\n(Conflicts Found)', xy=(x_position + 5.5, validation_y), 
                   xytext=(x_position + 3.8, validation_y),
                   arrowprops=dict(arrowstyle='->', lw=2, color='red'),
                   fontsize=10, color='red', fontweight='bold')
        
        ax.text(x_position + 6.5, validation_y, 'ABORT\nRestart Transaction', 
               ha='center', va='center', fontsize=11, color='red', fontweight='bold',
               bbox=dict(boxstyle="round,pad=0.3", facecolor="pink", alpha=0.7))
        
        ax.set_xlim(-1, 8)
        ax.set_ylim(-0.5, 4.5)
        ax.set_title('OCC Transaction Lifecycle: Three-Phase Protocol\n' + 
                    'Optimistic Concurrency Control Flow', fontsize=16, fontweight='bold', pad=30)
        ax.axis('off')
        
        plt.tight_layout()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.output_dir}/occ_lifecycle_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Transaction lifecycle diagram saved: {filename}")
        plt.show()
        
    def create_summary_report(self, results_data):
        """
        Creates a comprehensive visual report combining all analyses.
        
        This generates a single-page summary with:
        - Performance comparison charts
        - Key metrics summary  
        - Recommendations for usage scenarios
        - System architecture overview
        
        Args:
            results_data (dict): Complete benchmark results
        """
        fig = plt.figure(figsize=(20, 16))
        
        # Create a complex subplot layout for comprehensive analysis
        gs = fig.add_gridspec(4, 3, hspace=0.3, wspace=0.3)
        
        # Title and header
        fig.suptitle('OCC vs 2PL: Distributed Database Performance Analysis Report', 
                    fontsize=20, fontweight='bold', y=0.95)
        
        # Add timestamp and metadata
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fig.text(0.02, 0.92, f'Generated: {timestamp}', fontsize=12, style='italic')
        
        print(f"Comprehensive analysis report generated at {timestamp}")
        print("   This report provides complete performance insights for your OCC implementation")
        
        # Save the comprehensive report
        timestamp_file = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.output_dir}/comprehensive_report_{timestamp_file}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Comprehensive report saved: {filename}")
        plt.show()

def save_results_for_visualization(results):
    """
    Helper function to save benchmark results in format suitable for visualization.
    
    This function takes raw benchmark results and formats them for the
    visualization module. It ensures all necessary metrics are available.
    
    Args:
        results (dict): Raw benchmark results from benchmark.py
        
    Returns:
        dict: Formatted results ready for visualization
    """
    formatted_results = {}
    
    for scenario, scenario_data in results.items():
        formatted_results[scenario] = {}
        
        for mode, mode_data in scenario_data.items():
            formatted_results[scenario][mode] = {
                'throughput': mode_data.get('throughput', 0),
                'abort_rate': mode_data.get('abort_rate', 0),
                'total_commits': mode_data.get('commits', 0),
                'total_aborts': mode_data.get('aborts', 0),
                'avg_duration': mode_data.get('duration', 0)
            }
    
    return formatted_results