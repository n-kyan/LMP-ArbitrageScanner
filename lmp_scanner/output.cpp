#include "scanner.h"
#include <fstream>
#include <iostream>
#include <iomanip>

void LMPScanner::write_node_rankings() {
    std::ofstream out("../output/node_rankings.csv");
    
    out << "pnode_id,zone,mean_spread,std_spread,sharpe_ratio,hit_rate,"
        << "sample_size,mean_abs_spread,net_profit_10mw,congestion_sharpe,"
        << "energy_sharpe,best_hour,best_hour_avg\n";
    
    out << std::fixed << std::setprecision(4);
    
    // Write top 100 nodes (or all if less than 100)
    int limit = std::min(100, static_cast<int>(results_.size()));
    for (int i = 0; i < limit; i++) {
        const auto& r = results_[i];
        out << r.pnode_id << ","
            << r.zone << ","
            << r.mean_spread << ","
            << r.std_spread << ","
            << r.sharpe_ratio << ","
            << r.hit_rate << ","
            << r.sample_size << ","
            << r.mean_abs_spread << ","
            << r.net_profit_10mw << ","
            << r.congestion_sharpe << ","
            << r.energy_sharpe << ","
            << r.best_hour << ","
            << r.best_hour_avg << "\n";
    }
    
    out.close();
    std::cout << "  ✓ node_rankings.csv (top " << limit << " nodes)" << std::endl;
}

void LMPScanner::write_zone_summary() {
    std::ofstream out("../output/zone_summary.csv");
    
    out << "zone,avg_sharpe,num_profitable_nodes,total_samples\n";
    out << std::fixed << std::setprecision(4);
    
    for (const auto& z : zone_summaries_) {
        out << z.zone << ","
            << z.avg_sharpe << ","
            << z.num_profitable_nodes << ","
            << z.total_samples << "\n";
    }
    
    out.close();
    std::cout << "  ✓ zone_summary.csv" << std::endl;
}

void LMPScanner::write_component_analysis() {
    std::ofstream out("../output/component_analysis.csv");
    
    out << "pnode_id,zone,total_sharpe,congestion_mean,congestion_std,congestion_sharpe,"
        << "energy_mean,energy_std,energy_sharpe\n";
    
    out << std::fixed << std::setprecision(4);
    
    // Top 50 by congestion Sharpe
    auto sorted = results_;
    std::sort(sorted.begin(), sorted.end(),
              [](const NodeResult& a, const NodeResult& b) {
                  return a.congestion_sharpe > b.congestion_sharpe;
              });
    
    int limit = std::min(50, static_cast<int>(sorted.size()));
    for (int i = 0; i < limit; i++) {
        const auto& r = sorted[i];
        out << r.pnode_id << ","
            << r.zone << ","
            << r.sharpe_ratio << ","
            << r.congestion_mean << ","
            << r.congestion_std << ","
            << r.congestion_sharpe << ","
            << r.energy_mean << ","
            << r.energy_std << ","
            << r.energy_sharpe << "\n";
    }
    
    out.close();
    std::cout << "  ✓ component_analysis.csv (top 50 by congestion Sharpe)" << std::endl;
}

void LMPScanner::write_hourly_patterns() {
    // Aggregate across all nodes
    std::array<double, 24> hourly_spread_sum{};
    std::array<double, 24> hourly_variance_sum{};
    std::array<int, 24> hourly_obs{};
    
    for (const auto& [node_id, acc] : node_data_) {
        for (int h = 0; h < 24; h++) {
            if (acc.hourly_count[h] > 0) {
                hourly_spread_sum[h] += acc.hourly_sum[h];
                hourly_obs[h] += acc.hourly_count[h];
            }
        }
    }
    
    std::ofstream out("../output/hourly_patterns.csv");
    out << "hour,avg_spread,num_observations\n";
    out << std::fixed << std::setprecision(4);
    
    for (int h = 0; h < 24; h++) {
        double avg = hourly_obs[h] > 0 ? hourly_spread_sum[h] / hourly_obs[h] : 0.0;
        out << h << "," << avg << "," << hourly_obs[h] << "\n";
    }
    
    out.close();
    std::cout << "  ✓ hourly_patterns.csv" << std::endl;
}

void LMPScanner::write_summary_report() {
    std::ofstream out("../output/summary_report.txt");
    
    out << "═══════════════════════════════════════════════════════════════\n";
    out << "         LMP ARBITRAGE SCANNER - ANALYSIS RESULTS\n";
    out << "═══════════════════════════════════════════════════════════════\n\n";
    
    // Calculate some aggregate stats
    int total_nodes = node_data_.size();
    int profitable_nodes = results_.size();
    
    int total_obs = 0;
    for (const auto& [_, acc] : node_data_) {
        total_obs += acc.n;
    }
    
    out << "DATASET SUMMARY\n";
    out << "───────────────────────────────────────────────────────────────\n";
    out << "Total nodes analyzed:        " << total_nodes << "\n";
    out << "Profitable nodes:            " << profitable_nodes << "\n";
    out << "Total observations:          " << total_obs << "\n";
    out << "Transaction cost filter:     $" << std::fixed << std::setprecision(2) 
        << transaction_cost_ << "/MWh\n\n";
    
    out << "TOP 20 NODES BY SHARPE RATIO\n";
    out << "───────────────────────────────────────────────────────────────\n";
    out << std::setw(4) << "#" << " " 
        << std::setw(10) << "Node ID" << " "
        << std::setw(8) << "Zone" << " "
        << std::setw(8) << "Sharpe" << " "
        << std::setw(8) << "Mean $" << " "
        << std::setw(8) << "StdDev" << " "
        << std::setw(8) << "Hit%" << "\n";
    
    int limit = std::min(20, static_cast<int>(results_.size()));
    for (int i = 0; i < limit; i++) {
        const auto& r = results_[i];
        out << std::setw(4) << (i + 1) << " "
            << std::setw(10) << r.pnode_id << " "
            << std::setw(8) << r.zone << " "
            << std::setw(8) << std::fixed << std::setprecision(2) << r.sharpe_ratio << " "
            << std::setw(8) << std::fixed << std::setprecision(2) << r.mean_spread << " "
            << std::setw(8) << std::fixed << std::setprecision(2) << r.std_spread << " "
            << std::setw(7) << std::fixed << std::setprecision(1) << (r.hit_rate * 100) << "%\n";
    }
    
    out << "\nZONE RANKINGS\n";
    out << "───────────────────────────────────────────────────────────────\n";
    out << std::setw(4) << "#" << " "
        << std::setw(12) << "Zone" << " "
        << std::setw(10) << "Avg Sharpe" << " "
        << std::setw(10) << "# Nodes" << "\n";
    
    int zone_limit = std::min(10, static_cast<int>(zone_summaries_.size()));
    for (int i = 0; i < zone_limit; i++) {
        const auto& z = zone_summaries_[i];
        out << std::setw(4) << (i + 1) << " "
            << std::setw(12) << z.zone << " "
            << std::setw(10) << std::fixed << std::setprecision(2) << z.avg_sharpe << " "
            << std::setw(10) << z.num_profitable_nodes << "\n";
    }
    
    // Key insights
    out << "\nKEY INSIGHTS\n";
    out << "───────────────────────────────────────────────────────────────\n";
    
    // Component analysis
    if (!results_.empty()) {
        double total_sharpe_sum = 0;
        double cong_sharpe_sum = 0;
        double energy_sharpe_sum = 0;
        
        for (const auto& r : results_) {
            total_sharpe_sum += std::abs(r.sharpe_ratio);
            cong_sharpe_sum += std::abs(r.congestion_sharpe);
            energy_sharpe_sum += std::abs(r.energy_sharpe);
        }
        
        double cong_contribution = cong_sharpe_sum / (cong_sharpe_sum + energy_sharpe_sum) * 100;
        
        out << "• Congestion component drives " << std::fixed << std::setprecision(1)
            << cong_contribution << "% of spread variance\n";
        
        // Find best hour
        std::array<double, 24> hourly_totals{};
        std::array<int, 24> hourly_counts{};
        
        for (const auto& [node_id, acc] : node_data_) {
            for (int h = 0; h < 24; h++) {
                hourly_totals[h] += std::abs(acc.hourly_sum[h]);
                hourly_counts[h] += acc.hourly_count[h];
            }
        }
        
        int best_hour = 0;
        double best_hour_activity = 0;
        for (int h = 0; h < 24; h++) {
            if (hourly_counts[h] > 0) {
                double avg = hourly_totals[h] / hourly_counts[h];
                if (avg > best_hour_activity) {
                    best_hour = h;
                    best_hour_activity = avg;
                }
            }
        }
        
        out << "• Peak spread volatility at hour " << best_hour << ":00\n";
        
        // Profitability estimate
        double total_profit_10mw = 0;
        for (const auto& r : results_) {
            total_profit_10mw += r.net_profit_10mw;
        }
        
        // Calculate daily average (3 months = ~90 days)
        double days = 90.0;
        out << "• Estimated profit (10MW positions): $" 
            << std::fixed << std::setprecision(0) << total_profit_10mw 
            << " total ($" << (total_profit_10mw / days) << "/day avg)\n";
    }
    
    out << "\n═══════════════════════════════════════════════════════════════\n";
    out << "Analysis complete. See CSV files for detailed results.\n";
    out << "═══════════════════════════════════════════════════════════════\n";
    
    out.close();
    std::cout << "  ✓ summary_report.txt" << std::endl;
}