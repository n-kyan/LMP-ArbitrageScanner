#pragma once
#include <cstdlib>
#include <cstring>

// Ultra-fast CSV parser - zero allocations, direct parsing
struct FastCSVParser {
    const char* data;
    size_t len;
    size_t pos;
    
    FastCSVParser(const char* line, size_t length) 
        : data(line), len(length), pos(0) {}
    
    // Skip to next field
    inline void skip() {
        while (pos < len && data[pos] != ',') pos++;
        if (pos < len) pos++; // Skip comma
    }
    
    // Parse integer
    inline int parse_int() {
        while (pos < len && data[pos] == ',') pos++;
        
        int val = 0;
        bool neg = data[pos] == '-';
        if (neg) pos++;
        
        while (pos < len && data[pos] >= '0' && data[pos] <= '9') {
            val = val * 10 + (data[pos++] - '0');
        }
        
        return neg ? -val : val;
    }
    
    // Parse double using strtod (fastest standard method)
    inline double parse_double() {
        while (pos < len && data[pos] == ',') pos++;
        
        char* end;
        double val = strtod(data + pos, &end);
        pos = end - data;
        return val;
    }
    
    // Parse string (copies to output)
    inline void parse_string(char* out, size_t max_len) {
        while (pos < len && data[pos] == ',') pos++;
        
        size_t i = 0;
        while (pos < len && data[pos] != ',' && i < max_len - 1) {
            out[i++] = data[pos++];
        }
        out[i] = '\0';
    }
};

// Optimized row parser for your specific CSV format
struct CSVRowParser {
    static inline bool parse(const char* line, size_t len,
                            int& pnode_id, char* zone, double& spread,
                            double& cong_da, double& cong_rt,
                            double& energy_da, double& energy_rt,
                            int& hour) {
        FastCSVParser p(line, len);
        
        // Skip to column 7 (congestion_da)
        for (int i = 0; i < 7; i++) p.skip();
        cong_da = p.parse_double();
        
        p.skip(); // loss_da (col 8)
        energy_da = p.parse_double(); // col 9
        
        // Skip to column 17 (congestion_rt)
        for (int i = 0; i < 7; i++) p.skip();
        cong_rt = p.parse_double();
        
        p.skip(); // loss_rt (col 18)
        energy_rt = p.parse_double(); // col 19
        
        // Parse datetime for hour (col 20)
        p.skip();
        // Extract hour from datetime string "YYYY-MM-DD HH:MM:SS"
        // Fast forward to space, then parse HH
        const char* dt_start = line + p.pos;
        while (p.pos < len && line[p.pos] != ' ' && line[p.pos] != ',') p.pos++;
        if (p.pos < len && line[p.pos] == ' ') {
            p.pos++;
            hour = (line[p.pos] - '0') * 10 + (line[p.pos + 1] - '0');
        } else {
            hour = 0;
        }
        
        p.skip(); // Move past datetime
        
        // Parse pnode_id (col 21)
        pnode_id = p.parse_int();
        
        // Parse zone (col 22)
        p.parse_string(zone, 32);
        
        // Parse spread (col 23)
        spread = p.parse_double();
        
        return true;
    }
};