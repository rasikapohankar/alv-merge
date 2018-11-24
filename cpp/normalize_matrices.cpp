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
#include <boost/iostreams/device/file.hpp>
#include <chrono>

using namespace std;

int read_row_file(string path, unordered_map<string, int>& map) {
        string line;
        ifstream infile(path);
        int count = 0;

        while (getline(infile, line)) {
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

unordered_map<string, int> get_cb_counts(boost::filesystem::path path, int num_matrices) {
	vector<string> files = vector<string>();
	DIR *dp;
	struct dirent *dirp;
	unordered_map<string, int> map;
	boost::filesystem::path base_path;
	int count = 0;
	if ((dp = opendir(path.c_str())) == NULL) {
		cout << "Error opening dir\n";
	//	return;
	}

	while ((dirp = readdir(dp)) != NULL) {
		if (strcmp(dirp->d_name, ".") != 0 && 
			strcmp(dirp->d_name, "..") != 0) {
			base_path = path;
			base_path = base_path / dirp->d_name / dirp->d_name;
			base_path += "_rows.txt";
//			cout << base_path.string() << endl;
			int count = read_row_file(base_path.string(), map);
			count++;
		}
		if (count == num_matrices)
			break;
	}
	closedir(dp);
	return map;
}

int read_files(string matrix_path, vector<vector<double>>& cell_counts)
{
	time_t start, end;
	int num_genes = 52325; //TO-DO: read from _cols file
        int count = 0;

	time(&start);

	// open matrix file
	boost::iostreams::filtering_istream matrix_stream;
	matrix_stream.push(boost::iostreams::gzip_decompressor());
	matrix_stream.push(boost::iostreams::file_source(matrix_path,
							 ios_base::in | ios_base::binary));

	size_t elem_size = sizeof(typename vector<double>::value_type);
	vector<double> num_vector (num_genes, 0.0);
	double sum = 0.0;
	int c = 0;
	while (matrix_stream.read(reinterpret_cast<char *>(num_vector.data()), elem_size * num_genes)) {
		for (std::vector<double>::iterator it = num_vector.begin() ; it != num_vector.end(); ++it) {
			if (c < 10)
				cout << *it << endl;
			sum += *it;
			c++;		
	        }
		cell_counts.push_back(num_vector);
	}
	time(&end);
	cout << "Time for file " << matrix_path << ": " << end - start << " sec" << endl;
	return sum;
}


int concat_matrix(boost::filesystem::path path,
		  boost::filesystem::path& output_path,
		  unordered_map<string, int>& cb_counts,
		  int num_matrices
		) {
        DIR *dp;
        struct dirent *dirp;
	boost::filesystem::path base_path;
	int count = 0;
	double max = 0.0, sum = 0.0;
	int num_genes = 52325;
	unordered_map<string, vector<vector<double>>> cell_counts;
	cout << "dir in concat matrix " << path.string() << endl;

        if ((dp = opendir(path.c_str())) == NULL) {
                cout << "Error opening dir\n";
        //      return;
        }

	// open output matrix file
        auto matrix_out_path = output_path / "out_matrix.gz";
	boost::iostreams::filtering_ostream op_matrix_stream;
	op_matrix_stream.push(boost::iostreams::gzip_compressor());
	op_matrix_stream.push(boost::iostreams::file_sink(matrix_out_path.string(),
								ios_base::out | ios_base::binary));

	std::filebuf op_matrix;
	op_matrix.open(matrix_out_path.string(), std::ios::out);
	std::ostream op_file(&op_matrix);

	// open output rows file
        ofstream out_rows, out_cols;

        auto rows_path = output_path / "out_rows.txt";
        out_rows.open(rows_path.string());

	string cols_file;
        while ((dirp = readdir(dp)) != NULL) {
		if (strcmp(dirp->d_name, ".") != 0 && strcmp(dirp->d_name, "..") != 0) {
			base_path = path;
			base_path = base_path / dirp->d_name / dirp->d_name;
			base_path += ".gz";
			string matrix_path = base_path.string();

			base_path.remove_leaf();
			base_path = base_path / dirp->d_name;
			base_path += "_rows.txt";
			string rows_file = base_path.string();

			base_path.remove_leaf();
                        base_path = base_path / dirp->d_name;
                        base_path += "_cols.txt";
                        cols_file = base_path.string();

			vector<vector<double>> matrix_count;
			sum = read_files(matrix_path, matrix_count);
			cout << "matrix 1 sum " << sum << endl;
			cell_counts.insert({dirp->d_name, matrix_count});
			if (sum > max)
				max = sum;
                }
	}
	cout << "max sum is " << max << endl;
	size_t elem_size = sizeof(typename vector<double>::value_type);
	int k = 0;
	for (auto item : cell_counts) {
		for (auto row: item.second) {
			if (k < 10) {
				cout << "before: " << endl;
				for (int i = 0; i < 10; i++)
					cout << row[i] << "  ";
			}
			std::transform(row.begin(), row.end(), row.begin(),
			[&max](double c) -> double { return c / max; });
			if (k < 10) {
				cout << "after: " << endl;
				for (int i = 0; i < 10; i++)
                	                cout << row[i] << "  ";
			}
			k++;
//			std::bind(std::divides<T>(), std::placeholder::_1, max));
		}
		cout << "item first is: " << item.first << endl;
		auto matrix_out_path = output_path / item.first;
		matrix_out_path += ".gz";
		boost::iostreams::filtering_ostream op_matrix_stream;
		op_matrix_stream.push(boost::iostreams::gzip_compressor());
		op_matrix_stream.push(boost::iostreams::file_sink(matrix_out_path.string(),
                                                                ios_base::out | ios_base::binary));
		op_matrix_stream.write(reinterpret_cast<char *>((item.second).data()), elem_size * num_genes * (item.second).size());
	}

        closedir(dp);
	cout << "closed dir " << endl;
	cout << "closed out reads file" << endl;
	return 0;
}

int main(int argc, char *argv[]) {
	string s_path = argv[1];
	string s_output_path = argv[2];
	int num_matrices = std::stoi(argv[3]);
	time_t st, et;
	boost::filesystem::path in_path{s_path};
	boost::filesystem::path out_path{s_output_path};
	
	cout << "Getting cb counts" << endl;
        auto start = std::chrono::high_resolution_clock::now();
        unordered_map<string, int> cb_counts = get_cb_counts(in_path, num_matrices);
        auto end = std::chrono::high_resolution_clock::now();
        cout << "Got cb counts" << endl;
        cout << "Time " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " ms" << endl;

	cout << "Starting concatenation.." << endl;
	time(&st);
	auto s = std::chrono::high_resolution_clock::now();
	concat_matrix(in_path, out_path, cb_counts, num_matrices);
	time(&et);
	cout << "returned from concat_matrix" << endl;
	auto e = std::chrono::high_resolution_clock::now();
	cout << "Time to concatenate: " << std::chrono::duration_cast<std::chrono::milliseconds>(e - s).count() << " ms" << endl;
	cout << "Time with time: " << et - st << endl;
	cout << "exiting.." << endl;

	/*for (auto &x: cb_counts) {
		if (x.second > 0)
	        	cout << x.first << ":" << x.second << endl;
       	}*/
	return 0;
}

// These functions are not currently used
/*
int write_non_unique(unordered_map<std::string, vector<vector<double>>>& cell_counts,
                     boost::iostreams::filtering_ostream& op_matrix_stream,
                     ofstream& out_reads) {
        int count = 0;
        // write cell counts of repeated reads
        for (auto content: cell_counts) {
                for(auto &cell: content.second) {
                        out_reads << content.first << "_" << count << "\n";
                        op_matrix_stream.write(reinterpret_cast<char *>(num_vector.data()),
                                                                         elem_size * num_genes);
                        count++;
                }
        }
        return 0;
}

int write_unique(string s_read,
                     unordered_map<string, int> cb_counts,
                     unordered_map<std::string, vector<std::vector<double>>>& cell_counts,
                     boost::iostreams::filtering_ostream& op_matrix_stream,
                     ofstream& out_reads) {

        if (cb_counts.at(s_read) == 1) { // write to output file
                op_matrix_stream.write(reinterpret_cast<char *>(num_vector.data()), elem_size * num_genes);
                out_reads << s_read << "\n";
        } else { // keep in memory
                vector<vector<double>> nums;
                if (cell_counts.count(s_read) > 0) {
                        nums = cell_counts.at(s_read);
                        nums.push_back(num_vector);
                        cell_counts.at(s_read) = nums;
                } else {
                        nums.push_back(num_vector);
                        cell_counts.insert({s_read, nums});
                }
        }
        return 0;
}
*/
