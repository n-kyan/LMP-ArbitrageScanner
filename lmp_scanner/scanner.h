#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <array>
#include <cmath>
#include <algorithm>
#include <thread>
#include <mutex>
#include <atomic>

struct NodeAccumulator {
    int n = 0;
    double mean_spread = 0.0;
    double M2_spread = 0.0;
    
    double sum_abs_spread = 0.0;
    int positive_count = 0;
    
    double max_spread = -1e9;
    double min_spread = 1e9;
    
    double mean_cong_spread = 0.0;
    double M2_cong_spread = 0.0;
    double mean_energy_spread = 0.0;
    double M2_energy_spread = 0.0;
    
    std::array<double, 24> hourly_sum{};
    std::array<int, 24> hourly_count{};
    
    std::string zone;
    int pnode_id = 0;
    
    void update(double spread, double cong_spread, double energy_spread, 
                int hour, const std::string& zone_name, int node_id);
};

struct NodeResult {
    int pnode_id;
    std::string zone;
    int sample_size;
    
    double mean_spread;
    double std_spread;
    double sharpe_ratio;
    double hit_rate;
    double mean_abs_spread;
    
    double congestion_mean;
    double congestion_std;
    double congestion_sharpe;
    double energy_mean;
    double energy_std;
    double energy_sharpe;
    int best_hour;
    double best_hour_avg;
    
    double net_profit_10mw;
};

struct ZoneSummary {
    std::string zone;
    double avg_sharpe;
    int num_profitable_nodes;
    int total_samples;
};

struct CSVRow {
    int pnode_id;
    std::string zone;
    double spread;
    double congestion_da;
    double congestion_rt;
    double energy_da;
    double energy_rt;
    int hour;
    
    bool valid = false;
};

class LMPScanner {
public:
    LMPScanner(const std::string& csv_path, double transaction_cost = 0.75);
    
    void analyze();
    void write_results();
    
private:
    std::string csv_path_;
    double transaction_cost_;
    
    std::unordered_map<int, NodeAccumulator> node_data_;
    std::vector<NodeResult> results_;
    std::vector<ZoneSummary> zone_summaries_;
    
    CSVRow parse_line(const std::string& line);
    int extract_hour(const std::string& datetime_str);
    void calculate_results();
    void calculate_zone_summaries();
    
    void write_node_rankings();
    void write_zone_summary();
    void write_component_analysis();
    void write_hourly_patterns();
    void write_summary_report();
};