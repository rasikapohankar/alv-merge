#include <stdio.h>
#include <dirent.h>
#include <string>
#include <vector>
#include <errno.h>
#include <iostream>
#include <iomanip>
#include <unordered_map>
#include <cstring>
#include <fstream>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filtering_stream.hpp>

using namespace std;

char * make_file_name(const char *path, const char *dir_name, int size) {
	char *temp = (char *) std::malloc(std::strlen(path) + std::strlen(dir_name)*2 + size);
        std::strcpy(temp, path);
        std::strcat(temp, dir_name);
        std::strcat(temp, "/");
        std::strcat(temp, dir_name);
	return temp;
}


int read_row_file(char *path, std::unordered_map<std::string, int>& map) {
        const char *suffix = "_rows.txt";
        char *file_name = std::strcat(path, suffix);
        std::string line;
        cout << file_name << endl;
        std::ifstream infile(file_name);
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


std::unordered_map<std::string, int> get_cb_counts(std::string path) {
	vector<string> files = vector<string>();
	DIR *dp;
	struct dirent *dirp;
	std::unordered_map<std::string, int> map;

	if ((dp = opendir(path.c_str())) == NULL) {
		cout << "Error opening dir\n";
	//	return;
	}

	while ((dirp = readdir(dp)) != NULL) {
		if (std::strcmp(dirp->d_name, ".") != 0 && 
			std::strcmp(dirp->d_name, "..") != 0) {
			char *temp = make_file_name(path.c_str(), (const char*) dirp->d_name, 10);
			int count = read_row_file(temp, map);
			std::free(temp);
			cout << "read file" << endl;
		}
	}
	closedir(dp);
	return map;
}

int read_files(
		char *matrix_path,
		char *row_file_path,
		std::unordered_map<std::string, int> cb_counts,
		std::unordered_map<std::string, std::vector<std::vector<double>>>& cell_counts,
		std::ofstream& out_matrix,
		std::ofstream& out_reads
	      ) {
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
//	std::vector<std::vector<double>> nums;
	std::vector<double> num_vector (num_genes, 0.0);
	while (matrix_stream.read(reinterpret_cast<char *>(num_vector.data()), elem_size * num_genes)) {
//		nums.push_back(num_vector);
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

	// write cell counts of repeated reads
	for (auto content: cell_counts) {
		for(auto &cell: content.second) {
			out_reads << content.first << "\n";
			for (auto &val: cell)
				out_matrix << val << " ";
			out_matrix << "\n";
		}
	}
	return 0;
}

int concat_matrix(std::string path, std::unordered_map<std::string, int> cb_counts) {
        DIR *dp;
        struct dirent *dirp;
	const char *suffix = ".gz";
	const char *file_suffix = "_rows.txt";
	std::unordered_map<std::string, std::vector<std::vector<double>>> cell_counts;

        if ((dp = opendir(path.c_str())) == NULL) {
                cout << "Error opening dir\n";
        //      return;
        }

	// open output matrix file
        std::ofstream out_matrix;
        out_matrix.open("/media/rasika/My Passport/output/out_matrix.txt");

	// open output reads file
	std::ofstream out_reads;
	out_reads.open("/media/rasika/My Passport/output/out_reads.txt");

        while ((dirp = readdir(dp)) != NULL) {
		if (std::strcmp(dirp->d_name, ".") != 0 && std::strcmp(dirp->d_name, "..") != 0) {
			char *temp = make_file_name(path.c_str(), (const char *) dirp->d_name, 4);
        		char *matrix_path = std::strcat(temp, suffix);
			cout << matrix_path << endl;

			char *file_temp = make_file_name(path.c_str(), (const char *) dirp->d_name, 10);
			char *file_path = std::strcat(file_temp, file_suffix);
			cout << file_path << endl;

			read_files(matrix_path, file_path, cb_counts, cell_counts, out_matrix, out_reads);
			std::free(temp);
			std::free(file_temp);
                }
	}
        closedir(dp);
	cout << "closed dir " << endl;
	out_matrix.close();
	out_reads.close();
	return 0;
}

int main() {
	std::string path = "../alv_matrices/";
	std::unordered_map<std::string, int> cb_counts = get_cb_counts(path);
	
	concat_matrix(path, cb_counts);
	cout << "exiting.." << endl;
	return 0;
}
