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
#include <immintrin.h>

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

// Pack up to 8 chars into a uint64_t using little-endian byte order.
// memcpy into a zero-initialised word compiles to a single mov on x86 for 8-byte inputs.
// Unique for HCPCS codes since they contain no embedded null bytes.
inline uint64_t string_to_key(const std::string& s) {
    uint64_t key = 0;
    memcpy(&key, s.data(), std::min(s.size(), size_t(8)));
    return key;
}

// SIMD delimiter search: scan 32 bytes at a time (AVX2) or 16 bytes (SSE2 fallback).
// Returns pointer to first matching byte, or end if not found.
#ifdef __AVX2__
inline const char* find_newline_simd(const char* ptr, const char* end) {
    const __m256i nl = _mm256_set1_epi8('\n');
    while (ptr + 32 <= end) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
        unsigned mask = static_cast<unsigned>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, nl)));
        if (mask != 0) return ptr + __builtin_ctz(mask);
        ptr += 32;
    }
    while (ptr < end && *ptr != '\n') ++ptr;
    return ptr;
}

#else
inline const char* find_newline_simd(const char* ptr, const char* end) {
    const __m128i nl = _mm_set1_epi8('\n');
    while (ptr + 16 <= end) {
        __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr));
        unsigned mask = static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(chunk, nl)));
        if (mask != 0) return ptr + __builtin_ctz(mask);
        ptr += 16;
    }
    while (ptr < end && *ptr != '\n') ++ptr;
    return ptr;
}
#endif

// Single-pass comma scan: read each chunk once, drain all comma positions from its
// bitmask before advancing. 6 separate find_comma_simd calls would reload the same
// chunk repeatedly; this touches each byte at most once.
ReducedMedicareRecord parse_medicate_record(const char* line_start, const char* line_end) {
    const char* commas[6];
    int found = 0;
    const char* ptr = line_start;

#ifdef __AVX2__
    const __m256i cm = _mm256_set1_epi8(',');
    while (ptr + 32 <= line_end && found < 6) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
        unsigned mask = static_cast<unsigned>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, cm)));
        while (mask != 0 && found < 6) {
            commas[found++] = ptr + __builtin_ctz(mask);
            mask &= mask - 1;  // clear lowest set bit, advance to next comma in chunk
        }
        ptr += 32;
    }
#else
    const __m128i cm = _mm_set1_epi8(',');
    while (ptr + 16 <= line_end && found < 6) {
        __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr));
        unsigned mask = static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(chunk, cm)));
        while (mask != 0 && found < 6) {
            commas[found++] = ptr + __builtin_ctz(mask);
            mask &= mask - 1;
        }
        ptr += 16;
    }
#endif
    while (ptr < line_end && found < 6) {
        if (*ptr == ',') commas[found++] = ptr;
        ++ptr;
    }

    if (found < 6) return {"", 0, 0.0};

    const char* hcpcs_start = commas[1] + 1;
    std::string hcpcs_code(hcpcs_start, static_cast<size_t>(commas[2] - hcpcs_start));

    double total_paid = 0.0;
    std::from_chars(commas[5] + 1, line_end, total_paid);

    return {hcpcs_code, string_to_key(hcpcs_code), total_paid};
}

// Reverse SIMD newline search: scans backward from ptr toward base, returns pointer
// to the newline, or base if not found.
#ifdef __AVX2__
inline const char* find_newline_reverse_simd(const char* base, const char* ptr) {
    const __m256i nl = _mm256_set1_epi8('\n');
    while (ptr - 32 >= base) {
        ptr -= 32;
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
        unsigned mask = static_cast<unsigned>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, nl)));
        if (mask != 0) {
            // __builtin_clz finds highest set bit; highest bit = rightmost byte in memory
            return ptr + (31 - __builtin_clz(mask));
        }
    }
    while (ptr > base && *(ptr - 1) != '\n') --ptr;
    return ptr;
}
#else
inline const char* find_newline_reverse_simd(const char* base, const char* ptr) {
    const __m128i nl = _mm_set1_epi8('\n');
    while (ptr - 16 >= base) {
        ptr -= 16;
        __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr));
        unsigned mask = static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(chunk, nl)));
        if (mask != 0) {
            return ptr + (15 - __builtin_clz(mask << 16));
        }
    }
    while (ptr > base && *(ptr - 1) != '\n') --ptr;
    return ptr;
}
#endif

inline void align_region_start(const char* data_ptr, size_t& start, size_t end, bool should_align) {
    if (should_align) {
        const char* nl = find_newline_simd(data_ptr + start, data_ptr + end);
        start = static_cast<size_t>(nl - data_ptr) + 1;
    }
}

inline void align_region_end(const char* data_ptr, size_t& line_end, size_t start, bool should_align) {
    if (should_align) {
        const char* nl = find_newline_reverse_simd(data_ptr + start, data_ptr + line_end);
        line_end = static_cast<size_t>(nl - data_ptr);
    }
}

// Open-addressing flat hash map: uint64_t key → double value.
// Contiguous key/value arrays eliminate pointer chasing vs std::unordered_map's chained buckets.
// Fibonacci hashing (multiply by 2^64/phi) gives uniform bucket distribution.
// Sentinel: key 0 = empty slot — valid HCPCS keys are never 0 (codes are non-empty strings).
// Capacity must be a power of 2 and at least 2x the expected number of unique keys.
struct FlatDoubleMap {
    std::vector<uint64_t> keys_;
    std::vector<double>   values_;
    size_t mask_;

    explicit FlatDoubleMap(size_t capacity = 16384)
        : keys_(capacity, 0), values_(capacity, 0.0), mask_(capacity - 1) {}

    void add(uint64_t key, double val) {
        size_t idx = slot(key);
        while (keys_[idx] != 0 && keys_[idx] != key)
            idx = (idx + 1) & mask_;
        keys_[idx] = key;
        values_[idx] += val;
    }

    template <typename F>
    void for_each(F&& fn) const {
        for (size_t i = 0; i < keys_.size(); ++i)
            if (keys_[i] != 0) fn(keys_[i], values_[i]);
    }

private:
    size_t slot(uint64_t key) const {
        return (key * 11400714819323198485ULL) & mask_;
    }
};

inline void process_region_lines(const char* data_ptr, size_t start, size_t line_end,
                                  FlatDoubleMap& aggregated_data,
                                  std::unordered_map<uint64_t, std::string>& key_map) {
    size_t pos = start;
    while (pos < line_end) {
        const char* nl_ptr = find_newline_simd(data_ptr + pos, data_ptr + line_end);
        size_t next_newline = static_cast<size_t>(nl_ptr - data_ptr);

        ReducedMedicareRecord record = parse_medicate_record(data_ptr + pos, data_ptr + next_newline);
        aggregated_data.add(record.hcpcs_key, record.total_paid);
        // try_emplace: single lookup, only constructs string value when key is new
        key_map.try_emplace(record.hcpcs_key, record.hcpcs_code);

        pos = next_newline + 1;
    }
}

inline void merge_aggregated_data(FlatDoubleMap& global_data,
                                   std::unordered_map<uint64_t, std::string>& key_to_code,
                                   const FlatDoubleMap& local_data,
                                   const std::unordered_map<uint64_t, std::string>& local_key_map) {
    local_data.for_each([&](uint64_t key, double val) {
        global_data.add(key, val);
        auto it = local_key_map.find(key);
        if (it != local_key_map.end()) key_to_code.try_emplace(key, it->second);
    });
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

    FlatDoubleMap aggregated_data_combined;
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
        const char* data_ptr = static_cast<const char*>(file_data);
        const char* header_end = find_newline_simd(data_ptr, data_ptr + file_size);
        size_t data_start = static_cast<size_t>(header_end - data_ptr) + 1;

        // split file into chunks for each thread (line-aligned)
        size_t data_size = file_size - data_start;
        size_t chunk_size = data_size / num_threads;

        // Per-thread maps: each thread owns its slot, no locking needed during parse.
        std::vector<FlatDoubleMap> per_thread_data(num_threads);
        std::vector<std::unordered_map<uint64_t, std::string>> per_thread_keys(num_threads);
        for (unsigned int i = 0; i < num_threads; ++i) {
            per_thread_keys[i].reserve(512);
        }

        // Phase 1: parse — threads write only to their own slot, zero contention.
        #pragma omp parallel for num_threads(num_threads)
        for (unsigned int i = 0; i < num_threads; ++i) {
            g_timer.start("worker_" + std::to_string(i));

            size_t start = data_start + (i * chunk_size);
            size_t end = (i == num_threads - 1) ? file_size : (data_start + (i + 1) * chunk_size);

            align_region_start(data_ptr, start, end, i > 0);
            size_t line_end = end;
            align_region_end(data_ptr, line_end, start, i < num_threads - 1);

            process_region_lines(data_ptr, start, line_end, per_thread_data[i], per_thread_keys[i]);

            g_timer.stop();
        }

        // Phase 2: tree reduction — halve active threads each round.
        // O(log N) rounds with N/2 parallel merges each, vs O(N) serialised merges.
        for (unsigned int stride = 1; stride < num_threads; stride *= 2) {
            unsigned int limit = num_threads - stride;
            #pragma omp parallel for num_threads(num_threads)
            for (unsigned int i = 0; i < limit; i += 2 * stride) {
                merge_aggregated_data(per_thread_data[i], per_thread_keys[i],
                                      per_thread_data[i + stride], per_thread_keys[i + stride]);
            }
        }

        aggregated_data_combined = std::move(per_thread_data[0]);
        global_key_map = std::move(per_thread_keys[0]);
    } catch (...) {
        munmap(file_data, file_size);
        close(fd);
        throw;
    }

    munmap(file_data, file_size);
    close(fd);

    // Convert uint64_t keys back to strings for output
    std::vector<std::pair<std::string, double>> sorted_data;
    aggregated_data_combined.for_each([&](uint64_t key, double total_paid) {
        auto code_it = global_key_map.find(key);
        if (code_it != global_key_map.end()) {
            sorted_data.emplace_back(code_it->second, total_paid);
        }
    });

    // Sort aggregated data by total_paid (descending) before writing
    std::sort(sorted_data.begin(), sorted_data.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    // Create output file and write aggregated data
    std::ofstream output_file(args.output_path);
    if (!output_file.is_open()) {
        std::cerr << "Cannot open output file: " << args.output_path << std::endl;
        return 1;
    }
    output_file << "hcpcs_code,total_paid\n";
    output_file << std::fixed << std::setprecision(2);
    for (const auto& [hcpcs_code, total_paid] : sorted_data) {
        output_file << hcpcs_code << "," << total_paid << "\n";
    }
    output_file.close();
    std::cout << "Aggregation complete. Output written to: " << args.output_path << std::endl;

    // Stop main thread timing and report
    g_timer.stop();
    g_timer.report(args.timing_path);
    std::cout << "Timing report written to: " << args.timing_path << std::endl;

    return 0;

}

