#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <thread>
#include <filesystem>
#include <algorithm>
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
    std::istringstream ss(line);
    std::string token;
    std::vector<std::string> tokens;
    while (std::getline(ss, token, ',')) {
        tokens.push_back(token);
    }
    if (tokens.size() < 7) {
        throw std::runtime_error("Invalid record: " + line);
    }
    ReducedMedicareRecord record;
    record.hcpcs_code = tokens[2];
    record.total_paid = std::stod(tokens[6]);
    return record;
}

int main(int argc, char* argv[]) {
    // arg 1 is input path
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <input_path> [output_path]" << std::endl;
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

    // get number of cpus
    unsigned int num_cpus = std::thread::hardware_concurrency();
    std::cout << "Number of CPUs: " << num_cpus << std::endl;

    // get number of threads to use (cpus * 2) - 1
    unsigned int num_threads = (num_cpus * 2) - 1;
    std::cout << "Number of threads to use: " << num_threads << std::endl;

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
            // Calculate byte boundaries
            size_t start = data_start + (i * chunk_size);
            size_t end = (i == num_threads - 1) ? file_size : (data_start + (i + 1) * chunk_size);

            // Align start: skip to next newline (except for region 0 which already at line start)
            if (i > 0) {
                while (start < end && data_ptr[start] != '\n') {
                    ++start;
                }
                ++start;  // Move past the '\n' to start of next complete line
            }

            // Align end: back up to previous newline (except for last region)
            size_t line_end = end;
            if (i < num_threads - 1) {
                while (line_end > start && data_ptr[line_end - 1] != '\n') {
                    --line_end;
                }
            }

            // Process complete lines in [start, line_end)
            std::unordered_map<std::string, double> aggregated_data;
            size_t pos = start;

            while (pos < line_end) {
                // Find next newline
                size_t next_newline = pos;
                while (next_newline < line_end && data_ptr[next_newline] != '\n') {
                    ++next_newline;
                }

                // Extract and process line
                std::string line(data_ptr + pos, next_newline - pos);
                try {
                    ReducedMedicareRecord record = parse_medicate_record(line);
                    aggregated_data[record.hcpcs_code] += record.total_paid;
                } catch (const std::exception& e) {
                    std::cerr << "Error parsing line: " << e.what() << std::endl;
                }
                pos = next_newline + 1;
            }

            // Merge aggregated data into combined map
            #pragma omp critical
            {
                for (const auto& [hcpcs_code, total_paid] : aggregated_data) {
                    aggregated_data_combined[hcpcs_code] += total_paid;
                }
            }
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
    for (const auto& [hcpcs_code, total_paid] : sorted_data) {
        output_file << hcpcs_code << "," << total_paid << std::endl;
    }
    output_file.close();
    std::cout << "Aggregation complete. Output written to: " << output_path << std::endl;
    return 0;

}