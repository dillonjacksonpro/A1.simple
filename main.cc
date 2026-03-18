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
#include <atomic>
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
    const char* hcpcs_ptr;  // pointer into mmap'd buffer — no heap allocation
    size_t      hcpcs_len;
    uint64_t    hcpcs_key;
    int64_t     total_cents;
};

// Parse monetary string to integer cents without a per-character loop.
//
// Fast path (values up to $999,999.99 = 8 digits total):
//   1. Two memcpy calls compact integer + decimal digits into one 8-byte buffer,
//      skipping the dot entirely by addressing around it.
//   2. Masked SWAR subtraction removes ASCII '0' bias from all bytes at once.
//   3. Left-shift aligns digits so zero-padding is at the low end (required for SWAR).
//   4. Three rounds of parallel SWAR combine replace the serial *10 loop:
//        round 1: byte pairs  → 16-bit values  (lo*10 + hi)
//        round 2: 16-bit pairs → 32-bit values  (*100)
//        round 3: 32-bit halves → int64          (*10000)
//   The serial dependency depth drops from O(n digits) to O(log 8) = 3.
inline int64_t parse_cents(const char* ptr, const char* end) {
    const size_t len = static_cast<size_t>(end - ptr);

    if (len >= 3 && ptr[len - 3] == '.') {
        // Single 4-byte load catches "0.00" (0x30302E30 LE) in one compare.
        // Zero payments are common in Medicare data; skipping the SWAR path saves ~10 ops.
        if (len == 4) {
            uint32_t word;
            memcpy(&word, ptr, 4);
            if (word == 0x30302E30U) return 0;
        }

        const size_t int_len = len - 3;
        const size_t total   = int_len + 2;  // digit count without the dot

        if (total <= 8) {
            // Compact digits into an 8-byte zero-initialised buffer:
            // [integer digits | decimal digits | zero padding]
            char buf[8] = {};
            memcpy(buf,           ptr,           int_len);
            memcpy(buf + int_len, ptr + len - 2, 2);

            uint64_t v = 0;
            memcpy(&v, buf, 8);

            // Subtract ASCII '0' from each valid digit byte.
            // Mask prevents borrow propagation from the zero-padded tail bytes.
            // Branchless: when total==8 the mask is all-ones; ternary compiles to cmov.
            const uint64_t digit_sub  = 0x3030303030303030ULL;
            const uint64_t valid_mask = (total < 8u) ? ((1ULL << (total * 8u)) - 1u) : ~0ULL;
            v -= digit_sub & valid_mask;

            // Left-align: shift zero padding to the low bytes.
            // SWAR pair-combine requires leading zeros, not trailing zeros.
            // Branchless: shift is 0 when total==8, which is a safe no-op.
            v <<= static_cast<unsigned>((8u - total) * 8u);

            // Round 1: adjacent byte pairs → 16-bit values
            const uint64_t lo1 = v & 0x00FF00FF00FF00FFULL;
            const uint64_t hi1 = (v >> 8) & 0x00FF00FF00FF00FFULL;
            const uint64_t s1  = lo1 * 10 + hi1;

            // Round 2: adjacent 16-bit pairs → 32-bit values
            const uint64_t lo2 = s1 & 0x0000FFFF0000FFFFULL;
            const uint64_t hi2 = (s1 >> 16) & 0x0000FFFF0000FFFFULL;
            const uint64_t s2  = lo2 * 100 + hi2;

            // Round 3: two 32-bit halves → final cents value
            return static_cast<int64_t>((s2 & 0xFFFFFFFFULL) * 10000 + (s2 >> 32));
        }
    }

    // Fallback: values larger than $999,999.99 or non-standard decimal placement
    int64_t cents = 0;
    while (ptr < end && *ptr != '.')
        cents = cents * 10 + static_cast<int64_t>(*ptr++ - '0');
    cents *= 100;
    if (ptr < end && *ptr == '.') {
        ++ptr;
        if (ptr < end) { cents += static_cast<int64_t>(*ptr++ - '0') * 10; }
        if (ptr < end) { cents += static_cast<int64_t>(*ptr - '0'); }
    }
    return cents;
}

// Pack up to 8 chars into a uint64_t using little-endian byte order.
// memcpy into a zero-initialised word compiles to a single mov on x86 for 8-byte inputs.
// Unique for HCPCS codes since they contain no embedded null bytes.
inline uint64_t string_to_key(const char* s, size_t len) {
    uint64_t key = 0;
    memcpy(&key, s, std::min(len, size_t(8)));
    return key;
}

// SIMD newline search: process 64 bytes per iteration (AVX2) or 32 bytes (SSE2).
// Two loads are pipelined in-flight; m1|m2 collapses both results into one branch
// for the common "not found" case, avoiding a per-chunk misprediction.
// Returns pointer to first '\n', or end if not found.
#ifdef __AVX2__
inline const char* find_newline_simd(const char* ptr, const char* end) {
    const __m256i nl = _mm256_set1_epi8('\n');
    while (ptr + 64 <= end) {
        __m256i c1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
        __m256i c2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr + 32));
        unsigned m1 = static_cast<unsigned>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(c1, nl)));
        unsigned m2 = static_cast<unsigned>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(c2, nl)));
        if (m1 | m2) {
            if (m1) return ptr + __builtin_ctz(m1);
            return ptr + 32 + __builtin_ctz(m2);
        }
        ptr += 64;
    }
    // 32-byte tail for lines that don't fit in a 64-byte aligned chunk
    if (ptr + 32 <= end) {
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
    while (ptr + 32 <= end) {
        __m128i c1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr));
        __m128i c2 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr + 16));
        unsigned m1 = static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(c1, nl)));
        unsigned m2 = static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(c2, nl)));
        if (m1 | m2) {
            if (m1) return ptr + __builtin_ctz(m1);
            return ptr + 16 + __builtin_ctz(m2);
        }
        ptr += 32;
    }
    if (ptr + 16 <= end) {
        __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr));
        unsigned mask = static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(chunk, nl)));
        if (mask != 0) return ptr + __builtin_ctz(mask);
        ptr += 16;
    }
    while (ptr < end && *ptr != '\n') ++ptr;
    return ptr;
}
#endif

struct ParseResult {
    ReducedMedicareRecord record;
    const char*           next_line;  // points to char after '\n', or region_end if no '\n'
};

// Combined scan: finds '\n' (line end) and ',' (field boundaries) in one SIMD pass.
// Each 32-byte chunk is compared against both delimiters simultaneously, halving the
// number of loads vs. the previous separate find_newline_simd + parse_medicate_record.
// Valid Medicare lines never have '\n' in the 22-byte NPI prefix, so scanning from
// hcpcs_start correctly identifies the line boundary without missing it.
ParseResult parse_line(const char* line_start, const char* region_end) {
    static constexpr ptrdiff_t hcpcs_offset = 22;  // 10-digit NPI + ',' + 10-digit NPI + ','
    const char* hcpcs_start = line_start + hcpcs_offset;

    if (hcpcs_start >= region_end) {
        const char* nl = find_newline_simd(line_start, region_end);
        return {{nullptr, 0, 0, 0}, nl < region_end ? nl + 1 : region_end};
    }

#ifdef __AVX2__
    const __m256i nl_v = _mm256_set1_epi8('\n');
    const __m256i cm_v = _mm256_set1_epi8(',');

    // Lambda: extract hcpcs and total_paid from a finalised comma bitmask.
    auto extract = [&](uint64_t comma_mask, const char* line_end) -> ParseResult {
        if (comma_mask == 0) return {{nullptr, 0, 0, 0}, line_end + 1};
        const char* hcpcs_end        = hcpcs_start + __builtin_ctzll(comma_mask);
        const char* total_paid_start = hcpcs_start + (63u - static_cast<unsigned>(__builtin_clzll(comma_mask))) + 1u;
        if (total_paid_start <= hcpcs_end) return {{nullptr, 0, 0, 0}, line_end + 1};
        const size_t hlen = static_cast<size_t>(hcpcs_end - hcpcs_start);
        return {{hcpcs_start, hlen, string_to_key(hcpcs_start, hlen),
                 parse_cents(total_paid_start, line_end)}, line_end + 1};
    };

    if (hcpcs_start + 32 <= region_end) {
        __m256i        c0  = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hcpcs_start));
        const uint32_t nl0 = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(c0, nl_v)));
        uint32_t       cm0 = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(c0, cm_v)));

        if (nl0) {
            const unsigned p = static_cast<unsigned>(__builtin_ctz(nl0));
            if (p < 32u) cm0 &= (1u << p) - 1u;
            return extract(cm0, hcpcs_start + p);
        }

        if (hcpcs_start + 64 <= region_end) {
            __m256i        c1  = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(hcpcs_start + 32));
            const uint32_t nl1 = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(c1, nl_v)));
            uint32_t       cm1 = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(c1, cm_v)));

            if (nl1) {
                const unsigned p = static_cast<unsigned>(__builtin_ctz(nl1));
                if (p < 32u) cm1 &= (1u << p) - 1u;
                return extract(static_cast<uint64_t>(cm0) | (static_cast<uint64_t>(cm1) << 32),
                               hcpcs_start + 32 + p);
            }
        }
    }
#else
    const __m128i nl_v = _mm_set1_epi8('\n');
    const __m128i cm_v = _mm_set1_epi8(',');

    auto extract = [&](uint32_t comma_mask, const char* line_end) -> ParseResult {
        if (comma_mask == 0) return {{nullptr, 0, 0, 0}, line_end + 1};
        const char* hcpcs_end        = hcpcs_start + __builtin_ctz(comma_mask);
        const char* total_paid_start = hcpcs_start + (31u - static_cast<unsigned>(__builtin_clz(comma_mask))) + 1u;
        if (total_paid_start <= hcpcs_end) return {{nullptr, 0, 0, 0}, line_end + 1};
        const size_t hlen = static_cast<size_t>(hcpcs_end - hcpcs_start);
        return {{hcpcs_start, hlen, string_to_key(hcpcs_start, hlen),
                 parse_cents(total_paid_start, line_end)}, line_end + 1};
    };

    if (hcpcs_start + 16 <= region_end) {
        __m128i        c0  = _mm_loadu_si128(reinterpret_cast<const __m128i*>(hcpcs_start));
        const uint32_t nl0 = static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(c0, nl_v)));
        uint32_t       cm0 = static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(c0, cm_v)));

        if (nl0) {
            const unsigned p = static_cast<unsigned>(__builtin_ctz(nl0));
            if (p < 16u) cm0 &= (1u << p) - 1u;
            return extract(cm0, hcpcs_start + p);
        }

        if (hcpcs_start + 32 <= region_end) {
            __m128i        c1  = _mm_loadu_si128(reinterpret_cast<const __m128i*>(hcpcs_start + 16));
            const uint32_t nl1 = static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(c1, nl_v)));
            uint32_t       cm1 = static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(c1, cm_v)));

            if (nl1) {
                const unsigned p = static_cast<unsigned>(__builtin_ctz(nl1));
                if (p < 16u) cm1 &= (1u << p) - 1u;
                return extract(cm0 | (cm1 << 16), hcpcs_start + 16 + p);
            }
        }
    }
#endif

    // Slow path: line > 64 bytes from hcpcs_start (AVX2) / > 32 bytes (SSE2), or
    // no '\n' found in the scan window. Valid lines have no '\n' before byte 22,
    // so starting at hcpcs_start still finds the correct line boundary.
    const char* line_end = find_newline_simd(hcpcs_start, region_end);
    const char* hcpcs_end = hcpcs_start;
    while (hcpcs_end < line_end && *hcpcs_end != ',') ++hcpcs_end;
    if (hcpcs_end >= line_end) {
        return {{nullptr, 0, 0, 0}, line_end < region_end ? line_end + 1 : region_end};
    }
    const char* total_paid_start = line_end;
    while (*(total_paid_start - 1) != ',') --total_paid_start;
    if (total_paid_start <= hcpcs_end) {
        return {{nullptr, 0, 0, 0}, line_end < region_end ? line_end + 1 : region_end};
    }
    const size_t hlen = static_cast<size_t>(hcpcs_end - hcpcs_start);
    return {{hcpcs_start, hlen, string_to_key(hcpcs_start, hlen),
             parse_cents(total_paid_start, line_end)},
            line_end < region_end ? line_end + 1 : region_end};
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

inline void align_region_end(const char* data_ptr, size_t& line_end, size_t file_size, bool should_align) {
    if (should_align) {
        // Scan forward from the raw split point to find the next newline.
        // Both align_region_end (thread i) and align_region_start (thread i+1)
        // scan forward from the same boundary, so they land on the same newline
        // with no gap and no double-count.
        const char* nl = find_newline_simd(data_ptr + line_end, data_ptr + file_size);
        line_end = static_cast<size_t>(nl - data_ptr);
    }
}

// Open-addressing flat hash map: uint64_t key → int64_t value.
// Key and value are interleaved in one Entry struct (16 bytes = one cache line slot),
// so a single cache miss covers both — split arrays would require two misses per lookup.
// Fibonacci hashing (multiply by 2^64/phi) gives uniform bucket distribution.
// Sentinel: key 0 = empty slot — valid HCPCS keys are never 0 (codes are non-empty strings).
// Capacity must be a power of 2 and at least 2x the expected number of unique keys.
// 16384 entries × 16 bytes = 256 KB — fits in L2 cache per core on Skylake+ and newer.
// Supports up to 12288 unique keys (75% load). Increase if the dataset has more unique codes.
struct FlatDoubleMap {
    struct Entry {
        uint64_t key   = 0;
        int64_t  value = 0;
    };
    std::vector<Entry> entries_;
    size_t mask_;
    size_t count_ = 0;

    explicit FlatDoubleMap(size_t capacity = 16384)
        : entries_(capacity), mask_(capacity - 1) {}

    void add(uint64_t key, int64_t val) {
        size_t idx = slot(key);
        while (entries_[idx].key != 0 && entries_[idx].key != key)
            idx = (idx + 1) & mask_;
        if (entries_[idx].key == 0) ++count_;
        entries_[idx].key    = key;
        entries_[idx].value += val;
    }

    template <typename F>
    void for_each(F&& fn) const {
        for (const auto& e : entries_)
            if (e.key != 0) fn(e.key, e.value);
    }

private:
    size_t slot(uint64_t key) const {
        return (key * 11400714819323198485ULL) & mask_;
    }
};

inline void merge_aggregated_data(FlatDoubleMap& global_data, const FlatDoubleMap& local_data) {
    local_data.for_each([&](uint64_t key, int64_t val) {
        global_data.add(key, val);
    });
}

inline void process_region_lines(const char* data_ptr, size_t start, size_t region_end,
                                  FlatDoubleMap& aggregated_data) {
    size_t pos = start;
    while (pos < region_end) {
        ParseResult result = parse_line(data_ptr + pos, data_ptr + region_end);
        if (result.record.hcpcs_ptr != nullptr)
            aggregated_data.add(result.record.hcpcs_key, result.record.total_cents);
        pos = static_cast<size_t>(result.next_line - data_ptr);
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

    FlatDoubleMap aggregated_data_combined;

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

        // Phase 1: parse — threads write only to their own slot, zero contention.
        #pragma omp parallel for num_threads(num_threads)
        for (unsigned int i = 0; i < num_threads; ++i) {
            g_timer.start("worker_" + std::to_string(i));

            size_t start = data_start + (i * chunk_size);
            size_t end = (i == num_threads - 1) ? file_size : (data_start + (i + 1) * chunk_size);

            align_region_start(data_ptr, start, end, i > 0);
            size_t line_end = end;
            align_region_end(data_ptr, line_end, file_size, i < num_threads - 1);

            process_region_lines(data_ptr, start, line_end, per_thread_data[i]);

            g_timer.stop();
        }

        // Phase 2: tree reduction — halve active threads each round.
        // O(log N) rounds with N/2 parallel merges each, vs O(N) serialised merges.
        for (unsigned int stride = 1; stride < num_threads; stride *= 2) {
            unsigned int limit = num_threads - stride;
            #pragma omp parallel for num_threads(num_threads)
            for (unsigned int i = 0; i < limit; i += 2 * stride) {
                merge_aggregated_data(per_thread_data[i], per_thread_data[i + stride]);
            }
        }

        aggregated_data_combined = std::move(per_thread_data[0]);
    } catch (...) {
        munmap(file_data, file_size);
        close(fd);
        throw;
    }

    munmap(file_data, file_size);
    close(fd);

    // Recover HCPCS string from packed key: string_to_key wrote bytes via memcpy,
    // so the inverse is another memcpy + strnlen to find the null terminator.
    std::vector<std::pair<std::string, int64_t>> sorted_data;
    aggregated_data_combined.for_each([&](uint64_t key, int64_t total_paid) {
        char code[8];
        memcpy(code, &key, 8);
        sorted_data.emplace_back(std::string(code, strnlen(code, 8)), total_paid);
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
    for (const auto& [hcpcs_code, total_cents] : sorted_data) {
        output_file << hcpcs_code << ","
                    << total_cents / 100 << "."
                    << std::setw(2) << std::setfill('0') << total_cents % 100 << "\n";
    }
    output_file.close();
    std::cout << "Aggregation complete. Output written to: " << args.output_path << std::endl;

    // Stop main thread timing and report
    g_timer.stop();
    g_timer.report(args.timing_path);
    std::cout << "Timing report written to: " << args.timing_path << std::endl;

    return 0;

}

