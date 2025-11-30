#include "scanner.h"
#include <iostream>
#include <chrono>

int main(int argc, char* argv[]) {
    try {
        std::string csv_path = "lmp_data_merged.csv";
        double transaction_cost = 0.75;
        
        // Parse command line arguments
        if (argc > 1) {
            csv_path = argv[1];
        }
        if (argc > 2) {
            transaction_cost = std::stod(argv[2]);
        }
        
        std::cout << "═══════════════════════════════════════════════════════════\n";
        std::cout << "           LMP ARBITRAGE SCANNER v1.0\n";
        std::cout << "═══════════════════════════════════════════════════════════\n\n";
        
        auto start = std::chrono::high_resolution_clock::now();
        
        LMPScanner scanner(csv_path, transaction_cost);
        scanner.analyze();
        scanner.write_results();
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);
        
        std::cout << "\n═══════════════════════════════════════════════════════════\n";
        std::cout << "Total runtime: " << duration.count() << " seconds\n";
        std::cout << "═══════════════════════════════════════════════════════════\n";
        
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}