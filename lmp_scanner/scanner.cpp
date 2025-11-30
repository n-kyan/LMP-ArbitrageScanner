#include "scanner.h"
#include "fast_parser.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <iomanip>

void NodeAccumulator::update(double spread, double cong_spread, double energy_spread,
                             int hour, const std::string& zone_name, int node_id) {
    n++;
    
    if (n == 1) {
        zone = zone_name;
        pnode_id = node_id;
    }
    
    // Welford's algorithm for variance
    double delta = spread - mean_spread;
    mean_spread += delta / n;
    double delta2 = spread - mean_spread;
    M2_spread += delta * delta2;
    
    double cong_delta = cong_spread - mean_cong_spread;
    mean_cong_spread += cong_delta / n;
    double cong_delta2 = cong_spread - mean_cong_spread;
    M2_cong_spread += cong_delta * cong_delta2;
    
    double energy_delta = energy_spread - mean_energy_spread;
    mean_energy_spread += energy_delta / n;
    double energy_delta2 = energy_spread - mean_energy_spread;
    M2_energy_spread += energy_delta * energy_delta2;
    
    sum_abs_spread += std::abs(spread);
    if (spread > 0) positive_count++;
    
    max_spread = std::max(max_spread, spread);
    min_spread = std::min(min_spread, spread);
    
    if (hour >= 0 && hour < 24) {
        hourly_sum[hour] += spread;
        hourly_count[hour]++;
    }
}

LMPScanner::LMPScanner(const std::string& csv_path, double transaction_cost)
    : csv_path_(csv_path), transaction_cost_(transaction_cost) {}

int LMPScanner::extract_hour(const std::string& datetime_str) {
    auto space_pos = datetime_str.find(' ');
    if (space_pos == std::string::npos) return 0;
    
    auto time_part = datetime_str.substr(space_pos + 1);
    auto colon_pos = time_part.find(':');
    if (colon_pos == std::string::npos) return 0;
    
    try {
        return std::stoi(time_part.substr(0, colon_pos));
    } catch (...) {
        return 0;
    }
}

CSVRow LMPScanner::parse_line(const std::string& line) {
    CSVRow row;
    char zone_buf[32];
    
    bool success = CSVRowParser::parse(
        line.c_str(), line.size(),
        row.pnode_id, zone_buf, row.spread,
        row.congestion_da, row.congestion_rt,
        row.energy_da, row.energy_rt,
        row.hour
    );
    
    if (success) {
        row.zone = zone_buf;
        row.valid = true;
    } else {
        row.valid = false;
    }
    
    return row;
}

void LMPScanner::analyze() {
    std::ifstream file(csv_path_);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open CSV file: " + csv_path_);
    }
    
    std::cout << "Starting analysis of " << csv_path_ << "..." << std::endl;
    std::cout << "Transaction cost: $" << transaction_cost_ << "/MWh" << std::endl;
    
    // Skip header
    std::string line;
    std::getline(file, line);
    
    node_data_.reserve(15000);
    
    const int NUM_THREADS = std::thread::hardware_concurrency();
    std::cout << "Using " << NUM_THREADS << " threads..." << std::endl;
    
    std::vector<std::thread> threads;
    std::vector<std::vector<std::string>> line_chunks(NUM_THREADS);
    std::mutex merge_mutex;
    std::atomic<int> lines_processed{0};
    
    int chunk_idx = 0;
    const int CHUNK_SIZE = 100000;
    
    std::cout << "Reading file..." << std::endl;
    while (std::getline(file, line)) {
        line_chunks[chunk_idx % NUM_THREADS].push_back(std::move(line));
        
        if (++chunk_idx % (CHUNK_SIZE * NUM_THREADS) == 0) {
            std::cout << "  Loaded " << chunk_idx / 1000000 << "M rows..." << std::endl;
        }
    }
    file.close();
    
    std::cout << "Processing " << chunk_idx << " rows in parallel..." << std::endl;
    
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&, t]() {
            std::unordered_map<int, NodeAccumulator> local_data;
            local_data.reserve(15000);
            
            for (const auto& line : line_chunks[t]) {
                CSVRow row = parse_line(line);
                if (!row.valid) continue;
                
                double cong_spread = row.congestion_da - row.congestion_rt;
                double energy_spread = row.energy_da - row.energy_rt;
                
                local_data[row.pnode_id].update(
                    row.spread, cong_spread, energy_spread,
                    row.hour, row.zone, row.pnode_id
                );
            }
            
            // Merge into global
            std::lock_guard<std::mutex> lock(merge_mutex);
            for (auto& [node_id, local_acc] : local_data) {
                auto& global_acc = node_data_[node_id];
                
                if (global_acc.n == 0) {
                    global_acc = local_acc;
                } else {
                    int n_total = global_acc.n + local_acc.n;
                    double delta = local_acc.mean_spread - global_acc.mean_spread;
                    
                    global_acc.mean_spread = (global_acc.n * global_acc.mean_spread + 
                                             local_acc.n * local_acc.mean_spread) / n_total;
                    global_acc.M2_spread += local_acc.M2_spread + 
                                           delta * delta * global_acc.n * local_acc.n / n_total;
                    
                    double cong_delta = local_acc.mean_cong_spread - global_acc.mean_cong_spread;
                    global_acc.mean_cong_spread = (global_acc.n * global_acc.mean_cong_spread + 
                                                   local_acc.n * local_acc.mean_cong_spread) / n_total;
                    global_acc.M2_cong_spread += local_acc.M2_cong_spread + 
                                                 cong_delta * cong_delta * global_acc.n * local_acc.n / n_total;
                    
                    double energy_delta = local_acc.mean_energy_spread - global_acc.mean_energy_spread;
                    global_acc.mean_energy_spread = (global_acc.n * global_acc.mean_energy_spread + 
                                                     local_acc.n * local_acc.mean_energy_spread) / n_total;
                    global_acc.M2_energy_spread += local_acc.M2_energy_spread + 
                                                   energy_delta * energy_delta * global_acc.n * local_acc.n / n_total;
                    
                    global_acc.n = n_total;
                    global_acc.sum_abs_spread += local_acc.sum_abs_spread;
                    global_acc.positive_count += local_acc.positive_count;
                    global_acc.max_spread = std::max(global_acc.max_spread, local_acc.max_spread);
                    global_acc.min_spread = std::min(global_acc.min_spread, local_acc.min_spread);
                    
                    for (int h = 0; h < 24; h++) {
                        global_acc.hourly_sum[h] += local_acc.hourly_sum[h];
                        global_acc.hourly_count[h] += local_acc.hourly_count[h];
                    }
                }
            }
            
            lines_processed += line_chunks[t].size();
            std::cout << "  Thread " << t << " complete (" 
                      << line_chunks[t].size() << " rows)" << std::endl;
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    std::cout << "\nParsing complete:" << std::endl;
    std::cout << "  Total rows processed: " << lines_processed << std::endl;
    std::cout << "  Unique nodes: " << node_data_.size() << std::endl;
    
    std::cout << "\nCalculating statistics..." << std::endl;
    calculate_results();
    calculate_zone_summaries();
    
    std::cout << "Analysis complete!" << std::endl;
}

void LMPScanner::calculate_results() {
    const int MIN_SAMPLE_SIZE = 500;
    
    for (const auto& [node_id, acc] : node_data_) {
        if (acc.n < MIN_SAMPLE_SIZE) continue;
        
        NodeResult result;
        result.pnode_id = acc.pnode_id;
        result.zone = acc.zone.empty() ? "N/A" : acc.zone;
        result.sample_size = acc.n;
        
        result.mean_spread = acc.mean_spread;
        result.std_spread = std::sqrt(acc.M2_spread / acc.n);
        result.hit_rate = static_cast<double>(acc.positive_count) / acc.n;
        result.mean_abs_spread = acc.sum_abs_spread / acc.n;
        
        if (result.std_spread > 0) {
            // Sharpe ratio: mean/std (already per-hour)
            // Don't annualize - just use raw hourly Sharpe
            result.sharpe_ratio = result.mean_spread / result.std_spread;
        } else {
            result.sharpe_ratio = 0.0;
        }
        
        double tradeable_spread = std::max(0.0, std::abs(result.mean_spread) - transaction_cost_);
        result.net_profit_10mw = tradeable_spread * 10.0 * acc.n;
        
        result.congestion_mean = acc.mean_cong_spread;
        result.congestion_std = std::sqrt(acc.M2_cong_spread / acc.n);
        if (result.congestion_std > 0) {
            result.congestion_sharpe = result.congestion_mean / result.congestion_std;
        } else {
            result.congestion_sharpe = 0.0;
        }
        
        result.energy_mean = acc.mean_energy_spread;
        result.energy_std = std::sqrt(acc.M2_energy_spread / acc.n);
        if (result.energy_std > 0) {
            result.energy_sharpe = result.energy_mean / result.energy_std;
        } else {
            result.energy_sharpe = 0.0;
        }
        
        result.best_hour = 0;
        result.best_hour_avg = 0.0;
        for (int h = 0; h < 24; h++) {
            if (acc.hourly_count[h] > 0) {
                double avg = acc.hourly_sum[h] / acc.hourly_count[h];
                if (std::abs(avg) > std::abs(result.best_hour_avg)) {
                    result.best_hour = h;
                    result.best_hour_avg = avg;
                }
            }
        }
        
        if (std::abs(result.mean_spread) > transaction_cost_) {
            results_.push_back(result);
        }
    }
    
    std::sort(results_.begin(), results_.end(),
              [](const NodeResult& a, const NodeResult& b) {
                  return a.sharpe_ratio > b.sharpe_ratio;
              });
    
    std::cout << "  Profitable nodes (after transaction costs): " 
              << results_.size() << std::endl;
}

void LMPScanner::calculate_zone_summaries() {
    std::unordered_map<std::string, std::vector<double>> zone_sharpes;
    std::unordered_map<std::string, int> zone_counts;
    std::unordered_map<std::string, int> zone_samples;
    
    for (const auto& result : results_) {
        zone_sharpes[result.zone].push_back(result.sharpe_ratio);
        zone_counts[result.zone]++;
        zone_samples[result.zone] += result.sample_size;
    }
    
    for (const auto& [zone, sharpes] : zone_sharpes) {
        ZoneSummary summary;
        summary.zone = zone;
        summary.num_profitable_nodes = zone_counts[zone];
        summary.total_samples = zone_samples[zone];
        
        double sum = 0;
        for (double s : sharpes) sum += s;
        summary.avg_sharpe = sum / sharpes.size();
        
        zone_summaries_.push_back(summary);
    }
    
    std::sort(zone_summaries_.begin(), zone_summaries_.end(),
              [](const ZoneSummary& a, const ZoneSummary& b) {
                  return a.avg_sharpe > b.avg_sharpe;
              });
}

void LMPScanner::write_results() {
    std::cout << "\nWriting output files..." << std::endl;
    write_node_rankings();
    write_zone_summary();
    write_component_analysis();
    write_hourly_patterns();
    write_summary_report();
    std::cout << "All output files written successfully!" << std::endl;
}