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
    double total_paid;
};

ReducedMedicareRecord parse_medicate_record(const std::string& line) {
    // Manual CSV parsing: only extract fields 2 (hcpcs_code) and 6 (total_paid)
    size_t field_idx = 0;
    size_t field_start = 0;
    std::string hcpcs_code;
    double total_paid = 0.0;

    for (size_t i = 0; i <= line.size(); ++i) {
        if (i == line.size() || line[i] == ',') {
            // Process current field if needed
            if (field_idx == 2) {
                hcpcs_code = line.substr(field_start, i - field_start);
            } else if (field_idx == 6) {
                // Use from_chars for fast double parsing
                auto result = std::from_chars(line.data() + field_start, line.data() + i, total_paid);
                if (result.ec != std::errc{}) {
                    throw std::runtime_error("Invalid total_paid");
                }
                return {hcpcs_code, total_paid};  // Early return once we have both fields
            }

            field_idx++;
            field_start = i + 1;

            if (field_idx > 6) break;  // Stop after field 6
        }
    }

    throw std::runtime_error("Invalid record: insufficient fields");
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
                                  std::unordered_map<std::string, double>& aggregated_data) {
    size_t pos = start;
    while (pos < line_end) {
        size_t next_newline = pos;
        while (next_newline < line_end && data_ptr[next_newline] != '\n') {
            ++next_newline;
        }

        std::string line(data_ptr + pos, next_newline - pos);
        try {
            ReducedMedicareRecord record = parse_medicate_record(line);
            aggregated_data[record.hcpcs_code] += record.total_paid;
        } catch (const std::exception& e) {
            std::cerr << "Error parsing line: " << e.what() << std::endl;
        }
        pos = next_newline + 1;
    }
}

inline void merge_aggregated_data(std::unordered_map<std::string, double>& global_data,
                                   const std::unordered_map<std::string, double>& local_data) {
    for (const auto& [hcpcs_code, total_paid] : local_data) {
        global_data[hcpcs_code] += total_paid;
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

int main(int argc, char* argv[]) {
    g_timer.start("main");
    // arg 1 is input path
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <input_path> [output_path] [timing_path] [num_threads]" << std::endl;
        return 1;
    }
    std::string input_path = argv[1];
    // check if input is valid
    if (input_path.empty()) {
        std::cerr << "Input path cannot be empty." << std::endl;
        return 1;
    }
    // check if input exists
    if (!std::filesystem::exists(input_path)) {
        std::cerr << "Input path does not exist: " << input_path << std::endl;
        return 1;
    }
    std::string output_path = "output.csv";
    // arg 2 is output path
    if (argc >= 3) {
        output_path = argv[2];
    }
    // check if output path is valid
    if (output_path.empty()) {
        std::cerr << "Output path cannot be empty." << std::endl;
        return 1;
    }

    std::string timing_path = "timing.txt";
    // arg 3 is timing path
    if (argc >= 4) {
        timing_path = argv[3];
    }
    // check if timing path is valid
    if (timing_path.empty()) {
        std::cerr << "Timing path cannot be empty." << std::endl;
        return 1;
    }

    // get number of cpus
    unsigned int num_cpus = std::thread::hardware_concurrency();
    std::cout << "Number of CPUs: " << num_cpus << std::endl;

    // get number of threads to use
    // Default: (cpus * 2) - 1
    // Can be overridden via argv[4]
    unsigned int num_threads = (num_cpus * 2) - 1;
    if (argc >= 5) {
        int user_threads;
        const char* arg_start = argv[4];
        const char* arg_end = argv[4] + std::string(argv[4]).length();
        auto result = std::from_chars(arg_start, arg_end, user_threads);

        if (result.ec != std::errc{}) {
            std::cerr << "Error: Invalid num_threads value" << std::endl;
            return 1;
        }
        if (user_threads <= 0) {
            std::cerr << "Error: num_threads must be > 0" << std::endl;
            return 1;
        }
        num_threads = static_cast<unsigned int>(user_threads);
        std::cout << "Number of threads (user-specified): " << num_threads << std::endl;
    } else {
        std::cout << "Number of threads (default): " << num_threads << std::endl;
    }

    // create map to store aggregated data
    // key is hcpcs_code, value is total paid
    std::unordered_map<std::string, double> aggregated_data_combined;

    int fd = open(input_path.c_str(), O_RDONLY);
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

            // Align region boundaries to line boundaries
            align_region_start(data_ptr, start, end, i > 0);
            size_t line_end = end;
            align_region_end(data_ptr, line_end, start, i < num_threads - 1);

            // Process lines in this region
            std::unordered_map<std::string, double> aggregated_data;
            process_region_lines(data_ptr, start, line_end, aggregated_data);

            // Merge results into global aggregation
            #pragma omp critical
            {
                merge_aggregated_data(aggregated_data_combined, aggregated_data);
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

    // Sort aggregated data by total_paid (descending) before writing
    std::vector<std::pair<std::string, double>> sorted_data(aggregated_data_combined.begin(),
                                                             aggregated_data_combined.end());
    std::sort(sorted_data.begin(), sorted_data.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    // Create output file and write aggregated data
    std::ofstream output_file(output_path);
    if (!output_file.is_open()) {
        std::cerr << "Cannot open output file: " << output_path << std::endl;
        return 1;
    }
    output_file << "hcpcs_code,total_paid" << std::endl;
    output_file << std::fixed << std::setprecision(2);
    for (const auto& [hcpcs_code, total_paid] : sorted_data) {
        output_file << hcpcs_code << "," << total_paid << std::endl;
    }
    output_file.close();
    std::cout << "Aggregation complete. Output written to: " << output_path << std::endl;

    // Stop main thread timing and report
    g_timer.stop();
    g_timer.report(timing_path);
    std::cout << "Timing report written to: " << timing_path << std::endl;

    return 0;

}


// to do add seconds to the time, have main time at top
// add an arg to tell it how many cores it has