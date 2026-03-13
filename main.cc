#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <thread>
#include <filesystem>
#include <algorithm>
#include <iomanip>
#include <chrono>
#include <mutex>
#include <ctime>
#include <cstring>
#include <charconv>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

struct MedicareRecord {
    std::string billing_provider_npi;
    std::string servicing_provider_npi;
    std::string hcpcs_code;
    std::string claim_from_month;
    std::uint64_t total_unique_beneficiaries;
    std::uint64_t total_claims;
    double total_paid;
};

struct ReducedMedicareRecord {
    std::string hcpcs_code;
    uint64_t hcpcs_key;
    double total_paid;
};

// Convert string (≤8 chars) to uint64_t for hashing
inline uint64_t string_to_key(const std::string& s) {
    uint64_t key = 0;
    for (size_t i = 0; i < std::min(s.size(), size_t(8)); ++i) {
        key = (key << 8) | static_cast<uint8_t>(s[i]);
    }
    return key;
}

// Parse record directly from buffer to avoid string allocation
ReducedMedicareRecord parse_medicate_record(const char* line_start, const char* line_end) {
    // Manual CSV parsing: only extract fields 2 (hcpcs_code) and 6 (total_paid)
    size_t field_idx = 0;
    const char* field_start = line_start;
    std::string hcpcs_code;
    double total_paid = 0.0;

    for (const char* i = line_start; i <= line_end; ++i) {
        if (i == line_end || *i == ',') {
            // Process current field if needed
            if (field_idx == 2) {
                hcpcs_code.assign(field_start, static_cast<size_t>(i - field_start));
            } else if (field_idx == 6) {
                // Use from_chars for fast double parsing
                auto result = std::from_chars(field_start, i, total_paid);
                if (result.ec != std::errc{}) {
                    return {hcpcs_code, string_to_key(hcpcs_code), 0.0};
                }
                return {hcpcs_code, string_to_key(hcpcs_code), total_paid};
            }

            field_idx++;
            field_start = i + 1;

            if (field_idx > 6) break;  // Stop after field 6
        }
    }

    return {hcpcs_code, string_to_key(hcpcs_code), total_paid};
}

// Fine-grained inline functions for profiling
inline void align_region_start(const char* data_ptr, size_t& start, size_t end, bool should_align) {
    if (should_align) {
        while (start < end && data_ptr[start] != '\n') {
            ++start;
        }
        ++start;
    }
}

inline void align_region_end(const char* data_ptr, size_t& line_end, size_t start, bool should_align) {
    if (should_align) {
        while (line_end > start && data_ptr[line_end - 1] != '\n') {
            --line_end;
        }
    }
}

inline void process_region_lines(const char* data_ptr, size_t start, size_t line_end,
                                  std::unordered_map<uint64_t, double>& aggregated_data,
                                  std::unordered_map<uint64_t, std::string>& key_map) {
    size_t pos = start;
    while (pos < line_end) {
        size_t next_newline = pos;
        while (next_newline < line_end && data_ptr[next_newline] != '\n') {
            ++next_newline;
        }

        // Parse directly from buffer without creating temporary string
        ReducedMedicareRecord record = parse_medicate_record(data_ptr + pos, data_ptr + next_newline);
        aggregated_data[record.hcpcs_key] += record.total_paid;
        // Track original string for this key (only store once)
        if (key_map.find(record.hcpcs_key) == key_map.end()) {
            key_map[record.hcpcs_key] = record.hcpcs_code;
        }

        pos = next_newline + 1;
    }
}

inline void merge_aggregated_data(std::unordered_map<uint64_t, double>& global_data,
                                   std::unordered_map<uint64_t, std::string>& key_to_code,
                                   const std::unordered_map<uint64_t, double>& local_data,
                                   const std::unordered_map<uint64_t, std::string>& local_key_map) {
    for (const auto& [key, total_paid] : local_data) {
        global_data[key] += total_paid;
        // Store original string mapping if not already present
        if (key_to_code.find(key) == key_to_code.end()) {
            auto it = local_key_map.find(key);
            if (it != local_key_map.end()) {
                key_to_code[key] = it->second;
            }
        }
    }
}

class TimingTracker {
    struct ThreadTiming {
        std::string name;
        std::chrono::high_resolution_clock::time_point wall_start;
        clock_t cpu_start;
        double wall_elapsed_ms;
        double cpu_elapsed_ms;
    };

    std::unordered_map<std::thread::id, ThreadTiming> timings_;
    mutable std::mutex mutex_;

public:
    void start(const std::string& name = "") {
        auto id = std::this_thread::get_id();
        std::lock_guard<std::mutex> lock(mutex_);
        timings_[id] = {
            name,
            std::chrono::high_resolution_clock::now(),
            std::clock(),
            0.0,
            0.0
        };
    }

    void stop() {
        auto id = std::this_thread::get_id();
        auto now_wall = std::chrono::high_resolution_clock::now();
        auto now_cpu = std::clock();

        std::lock_guard<std::mutex> lock(mutex_);
        if (timings_.find(id) == timings_.end()) {
            return;
        }

        auto& timing = timings_[id];
        timing.wall_elapsed_ms = std::chrono::duration<double, std::milli>(
            now_wall - timing.wall_start).count();
        timing.cpu_elapsed_ms = static_cast<double>(now_cpu - timing.cpu_start) / CLOCKS_PER_SEC * 1000.0;
    }

    void report(const std::string& filepath) const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::ofstream timing_file(filepath);
        if (!timing_file.is_open()) {
            std::cerr << "Cannot open timing file: " << filepath << std::endl;
            return;
        }

        timing_file << "=== Timing Report ===" << std::endl;
        timing_file << std::left << std::setw(30) << "Thread/Task"
                    << std::setw(40) << "Wall Time"
                    << "CPU Time" << std::endl;
        timing_file << std::string(100, '-') << std::endl;

        // Create a sorted vector with main first
        std::vector<std::pair<std::string, const ThreadTiming*>> sorted_timings;
        std::string main_label;
        const ThreadTiming* main_timing = nullptr;

        for (const auto& [id, timing] : timings_) {
            std::string thread_label = "Thread " + std::to_string(std::hash<std::thread::id>{}(id) % 10000);
            if (!timing.name.empty()) {
                thread_label = timing.name;
            }

            if (thread_label == "main") {
                main_label = thread_label;
                main_timing = &timing;
            } else {
                sorted_timings.push_back({thread_label, &timing});
            }
        }

        // Lambda to format time with both ms and seconds
        auto format_time = [](double ms) -> std::string {
            std::ostringstream oss;
            oss << std::fixed << std::setprecision(2) << ms << " ms ("
                << std::fixed << std::setprecision(3) << (ms / 1000.0) << " s)";
            return oss.str();
        };

        // Print main first if it exists
        if (main_timing != nullptr) {
            timing_file << std::left << std::setw(30) << main_label
                        << std::setw(40) << format_time(main_timing->wall_elapsed_ms)
                        << format_time(main_timing->cpu_elapsed_ms) << std::endl;
        }

        // Sort remaining timings by label
        std::sort(sorted_timings.begin(), sorted_timings.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });

        // Print worker timings
        for (const auto& [label, timing] : sorted_timings) {
            timing_file << std::left << std::setw(30) << label
                        << std::setw(40) << format_time(timing->wall_elapsed_ms)
                        << format_time(timing->cpu_elapsed_ms) << std::endl;
        }
        timing_file.close();
    }
};

// Global timing tracker
TimingTracker g_timer;

struct CommandLineArguments {
    std::string input_path;
    std::string output_path = "output.csv";
    std::string timing_path = "timing.txt";
    unsigned int num_threads = 0;  // 0 means use default (cpus * 2 - 1)
};

CommandLineArguments parse_arguments(int argc, char* argv[]) {
    CommandLineArguments args;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if ((arg == "--input" || arg == "-i") && i + 1 < argc) {
            args.input_path = argv[++i];
        } else if ((arg == "--output" || arg == "-o") && i + 1 < argc) {
            args.output_path = argv[++i];
        } else if ((arg == "--timing" || arg == "-t") && i + 1 < argc) {
            args.timing_path = argv[++i];
        } else if ((arg == "--threads" || arg == "-n") && i + 1 < argc) {
            const char* threads_str = argv[++i];
            int threads_value;
            auto result = std::from_chars(threads_str, threads_str + std::strlen(threads_str), threads_value);
            if (result.ec != std::errc{} || threads_value <= 0) {
                throw std::runtime_error("Invalid --threads value: must be a positive integer");
            }
            args.num_threads = static_cast<unsigned int>(threads_value);
        } else if (arg == "--help" || arg == "-h") {
            throw std::runtime_error("Help requested");
        } else {
            throw std::runtime_error("Unknown argument: " + arg);
        }
    }

    if (args.input_path.empty()) {
        throw std::runtime_error("--input (or -i) is required");
    }

    return args;
}

int main(int argc, char* argv[]) {
    g_timer.start("main");

    // Parse command-line arguments
    CommandLineArguments args;
    try {
        args = parse_arguments(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        std::cerr << "\nUsage: " << argv[0] << " --input <path> [options]" << std::endl;
        std::cerr << "\nRequired arguments:\n";
        std::cerr << "  --input, -i <path>         Input CSV file path\n";
        std::cerr << "\nOptional arguments:\n";
        std::cerr << "  --output, -o <path>        Output CSV file path (default: output.csv)\n";
        std::cerr << "  --timing, -t <path>        Timing report file path (default: timing.txt)\n";
        std::cerr << "  --threads, -n <count>      Number of threads to use (default: cpus * 2 - 1)\n";
        std::cerr << "  --help, -h                 Show this help message\n";
        std::cerr << "\nExample:\n";
        std::cerr << "  " << argv[0] << " --input data.csv --output results.csv --threads 8\n";
        return 1;
    }

    // Validate input path
    if (!std::filesystem::exists(args.input_path)) {
        std::cerr << "Error: Input file does not exist: " << args.input_path << std::endl;
        return 1;
    }

    // Get number of CPUs and determine thread count
    unsigned int num_cpus = std::thread::hardware_concurrency();
    std::cout << "Number of CPUs: " << num_cpus << std::endl;

    unsigned int num_threads = args.num_threads;
    if (num_threads == 0) {
        num_threads = (num_cpus * 2) - 1;
        std::cout << "Number of threads (default): " << num_threads << std::endl;
    } else {
        std::cout << "Number of threads (user-specified): " << num_threads << std::endl;
    }

    // create map to store aggregated data with uint64_t keys for faster hashing
    // key is hcpcs_code (as uint64_t), value is total paid
    std::unordered_map<uint64_t, double> aggregated_data_combined;
    // Global mappings for key -> code (declared outside try block for use after)
    std::unordered_map<uint64_t, std::string> global_key_map;

    int fd = open(args.input_path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("Cannot open file");

    // get file size
    struct stat st;
    if (fstat(fd, &st) < 0) throw std::runtime_error("Cannot get file size");
    long int file_size_signed = st.st_size;
    if (file_size_signed < 0) {
        throw std::runtime_error("File size is negative");
    }
    size_t file_size = static_cast<size_t>(file_size_signed);

    // Check for empty file
    if (file_size == 0) {
        close(fd);
        throw std::runtime_error("Input file is empty");
    }

    void *file_data = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (file_data == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("Cannot map file");
    }

    try {
        // Skip header line (first line)
        size_t data_start = 0;
        const char* data_ptr = static_cast<const char*>(file_data);
        while (data_start < file_size && data_ptr[data_start] != '\n') {
            ++data_start;
        }
        ++data_start;  // Move past the '\n'

        // split file into chunks for each thread (line-aligned)
        size_t data_size = file_size - data_start;
        size_t chunk_size = data_size / num_threads;

        #pragma omp parallel for num_threads(num_threads)
        for (unsigned int i = 0; i < num_threads; ++i) {
            g_timer.start("worker_" + std::to_string(i));

            // Calculate byte boundaries
            size_t start = data_start + (i * chunk_size);
            size_t end = (i == num_threads - 1) ? file_size : (data_start + (i + 1) * chunk_size);

            // Thread-level prefetch: tell kernel to load this chunk (non-blocking)
            madvise(const_cast<char*>(data_ptr) + start, end - start, MADV_WILLNEED);

            // Align region boundaries to line boundaries
            align_region_start(data_ptr, start, end, i > 0);
            size_t line_end = end;
            align_region_end(data_ptr, line_end, start, i < num_threads - 1);

            // Process lines in this region with pre-allocated hash table (512 entries)
            std::unordered_map<uint64_t, double> aggregated_data;
            aggregated_data.reserve(512);
            std::unordered_map<uint64_t, std::string> local_key_map;
            local_key_map.reserve(512);

            process_region_lines(data_ptr, start, line_end, aggregated_data, local_key_map);

            // Merge results into global aggregation (uint64_t keyed)
            #pragma omp critical
            {
                merge_aggregated_data(aggregated_data_combined, global_key_map, aggregated_data, local_key_map);
            }

            g_timer.stop();
        }
    } catch (...) {
        munmap(file_data, file_size);
        close(fd);
        throw;
    }

    munmap(file_data, file_size);
    close(fd);

    // Convert uint64_t keys back to strings for output
    std::vector<std::pair<std::string, double>> sorted_data;
    for (const auto& [key, total_paid] : aggregated_data_combined) {
        auto code_it = global_key_map.find(key);
        if (code_it != global_key_map.end()) {
            sorted_data.emplace_back(code_it->second, total_paid);
        }
    }

    // Sort aggregated data by total_paid (descending) before writing
    std::sort(sorted_data.begin(), sorted_data.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    // Create output file and write aggregated data
    std::ofstream output_file(args.output_path);
    if (!output_file.is_open()) {
        std::cerr << "Cannot open output file: " << args.output_path << std::endl;
        return 1;
    }
    output_file << "hcpcs_code,total_paid" << std::endl;
    output_file << std::fixed << std::setprecision(2);
    for (const auto& [hcpcs_code, total_paid] : sorted_data) {
        output_file << hcpcs_code << "," << total_paid << std::endl;
    }
    output_file.close();
    std::cout << "Aggregation complete. Output written to: " << args.output_path << std::endl;

    // Stop main thread timing and report
    g_timer.stop();
    g_timer.report(args.timing_path);
    std::cout << "Timing report written to: " << args.timing_path << std::endl;

    return 0;

}

