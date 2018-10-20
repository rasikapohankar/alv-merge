#include <stdio.h>
#include <dirent.h>
#include <string>
#include <vector>
#include <iostream>
#include <unordered_map>
#include <cstring>
#include <fstream>
#include <time.h>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/filesystem.hpp>

using namespace std;


int read_row_file(std::string path, std::unordered_map<std::string, int>& map) {
        std::string line;
        std::ifstream infile(path);
        int count = 0;

        while (std::getline(infile, line)) {
                count++;
                if (map.count(line) > 0) {
                        map.at(line) = map.at(line) + 1;
                } else {
                        map.insert({line, 1});
                }
        }
        infile.close();
        return count;
}


std::unordered_map<std::string, int> get_cb_counts(boost::filesystem::path path) {
	vector<string> files = vector<string>();
	DIR *dp;
	struct dirent *dirp;
	std::unordered_map<std::string, int> map;
	boost::filesystem::path base_path;

	if ((dp = opendir(path.c_str())) == NULL) {
		cout << "Error opening dir\n";
	//	return;
	}

	while ((dirp = readdir(dp)) != NULL) {
		if (std::strcmp(dirp->d_name, ".") != 0 && 
			std::strcmp(dirp->d_name, "..") != 0) {
			base_path = path;
			base_path = base_path / dirp->d_name / dirp->d_name;
			base_path += "_rows.txt";
			cout << base_path.string() << endl;
			int count = read_row_file(base_path.string(), map);
		}
	}
	closedir(dp);
	return map;
}

int read_files(
		std::string matrix_path,
		std::string row_file_path,
		std::unordered_map<std::string, int> cb_counts,
		std::unordered_map<std::string, std::vector<std::vector<double>>>& cell_counts,
		std::ofstream& out_matrix,
		std::ofstream& out_reads
	      ) {
	time_t start, end;

	time(&start);
	// open file containing reads
	std::ifstream row_file(row_file_path, std::ios_base::in);
	std::string s_read;

	int num_genes = 52325;
	int i = 0;

	// open matrix file
	std::ifstream matrix_file(matrix_path, std::ios_base::in | std::ios_base::binary);
	boost::iostreams::filtering_istream matrix_stream;
	matrix_stream.push(boost::iostreams::gzip_decompressor());
	matrix_stream.push(matrix_file);

	size_t elem_size = sizeof(typename std::vector<double>::value_type);
	std::vector<double> num_vector (num_genes, 0.0);
	while (matrix_stream.read(reinterpret_cast<char *>(num_vector.data()), elem_size * num_genes)) {
		std::getline(row_file, s_read);
		if (cb_counts.at(s_read) == 1) {
			// write to output file
			for (auto& val: num_vector) {
				out_matrix << val << " ";
			}
			out_matrix << "\n";
			out_reads << s_read << "\n";
		} else {
			std::vector<std::vector<double>> nums;
			if (cell_counts.count(s_read) > 0) {
				nums = cell_counts.at(s_read);
				nums.push_back(num_vector);
				cell_counts.at(s_read) = nums;
			} else {
				nums.push_back(num_vector);
				cell_counts.insert({s_read, nums});
			}
		}
	}
	row_file.close();
	matrix_file.close();

	int count = 0;
	// write cell counts of repeated reads
	for (auto content: cell_counts) {
		for(auto &cell: content.second) {
			out_reads << content.first << "_" << count << "\n";
			for (auto &val: cell)
				out_matrix << val << " ";
			out_matrix << "\n";
			count++;
		}
	}
	time(&end);
	cout << "Time for file " << matrix_path << ": " << end - start << " sec" << endl;
	return 0;
}

int concat_matrix( boost::filesystem::path path,
		   boost::filesystem::path& output_path, 
		   std::unordered_map<std::string, int> cb_counts
		) {
        DIR *dp;
        struct dirent *dirp;
	const char *suffix = ".gz";
	const char *file_suffix = "_rows.txt";
	std::unordered_map<std::string, std::vector<std::vector<double>>> cell_counts;
	boost::filesystem::path base_path;

	cout << "dir in concat matrix " << path.string() << endl;
	
        if ((dp = opendir(path.c_str())) == NULL) {
                cout << "Error opening dir\n";
        //      return;
        }

	// open output matrix file
        std::ofstream out_matrix;
	auto matrix_out_path = output_path / "out_matrix.txt";
        out_matrix.open(matrix_out_path.string());

	// open output reads file
	std::ofstream out_reads;
	auto reads_path = output_path / "out_reads.txt";
	out_reads.open(reads_path.string());

        while ((dirp = readdir(dp)) != NULL) {
		if (std::strcmp(dirp->d_name, ".") != 0 && std::strcmp(dirp->d_name, "..") != 0) {
			base_path = path;
			base_path = base_path / dirp->d_name / dirp->d_name;
			base_path += ".gz";
			std::string matrix_path = base_path.string();

			base_path.remove_leaf();
			base_path = base_path / dirp->d_name;
			base_path += "_rows.txt";
			std::string file_path = base_path.string();

			read_files(matrix_path, file_path, cb_counts, cell_counts, out_matrix, out_reads);
                }
	}
        closedir(dp);
	cout << "closed dir " << endl;
	out_matrix.close();
	out_reads.close();
	return 0;
}

int main(int argc, char *argv[]) {
	std::string s_path = argv[1];
	std::string s_output_path = argv[2];

	boost::filesystem::path in_path{s_path};
	boost::filesystem::path out_path{s_output_path};
	
	time_t start, end;

	cout << "Getting cb counts" << endl;
	time (&start);
	std::unordered_map<std::string, int> cb_counts = get_cb_counts(in_path);
	time (&end);
	cout << "Got cb counts" << endl;
	cout << "Time " << end - start << " sec" << endl;

	cout << "Starting concatenation.." << endl;
	time(&start);
	concat_matrix(in_path, out_path, cb_counts);
	time(&end);
	cout << "Time to concatenate " << end - start << " sec" << endl;
	cout << "exiting.." << endl;
	return 0;
}
