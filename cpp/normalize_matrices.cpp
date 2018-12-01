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
#include <tuple>

using namespace std;
namespace io=boost::iostreams; 
namespace fs=boost::filesystem;

string get_path(fs::path base, string dir, string suffix) {
        base = base / dir / dir;
        base += suffix;
        return base.string();
}

int read_files(string matrix_path, vector<vector<double>>& cell_counts) {
        int num_genes = 52325; //TO-DO: read from _cols file
        int count = 0;

	auto start = std::chrono::high_resolution_clock::now();

        // open matrix file
        io::filtering_istream matrix_stream;
        matrix_stream.push(io::gzip_decompressor());
        matrix_stream.push(io::file_source(matrix_path, 
						ios_base::in | ios_base::binary));

        size_t elem_size = sizeof(typename vector<double>::value_type);
        vector<double> num_vector (num_genes, 0.0);
        double sum = 0.0;
        while (matrix_stream.read(reinterpret_cast<char *>(num_vector.data()), 
								elem_size * num_genes)) {
                for (vector<double>::iterator it = num_vector.begin(); it != num_vector.end(); ++it) {
                        sum += *it;
                }
                cell_counts.push_back(num_vector);
        }
	auto end = std::chrono::high_resolution_clock::now();
        cout << "Time for file: " << matrix_path << ": "
             << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
             << " ms" << endl;
	cout << "sum for " << matrix_path << ": " << sum << ", "
		"rpc: " << sum / cell_counts.size() << endl;
        return sum / cell_counts.size();
}

void open_stream(fs::path output_path,
			io::filtering_ostream& op_stream) {
        op_stream.push(io::gzip_compressor());
        op_stream.push(io::file_sink(output_path.string(),
                                ios_base::out | ios_base::binary));
}

void write_cols(fs::path path, string ip_path) {
	ofstream out_cols;
	string gene;
        
	out_cols.open(path.string());
        ifstream col_file(ip_path, ios_base::in);
        while (getline(col_file, gene)) {
                out_cols << gene << "\n";
        }
        out_cols.close();
}

void normalize(vector<tuple<string, vector<vector<double>>>> cell_counts,
		double fraction, vector<double> rpc, fs::path output_path) {
	size_t elem_size = sizeof(typename vector<double>::value_type);
	int num_genes = 52325;
	int index = rpc.at(0) < rpc.at(1) ? 0 : 1,
	    i = 0;

	vector<vector<double>> new_cell_counts;
        for (auto item: cell_counts) {
		if (i == index) {
                	for (auto row: get<1>(item)) {
                        	std::transform(row.begin(), row.end(), row.begin(),
	                        [&fraction](double c) -> 
					double { return c / fraction; });
				new_cell_counts.push_back(row);
        	        }
		}
                auto matrix_out_path = output_path / get<0>(item);
                matrix_out_path += ".gz";

		io::filtering_ostream op_stream;
		auto path = output_path / get<0>(item);
		path += ".gz";
	        open_stream(path, op_stream);
		int cnt = 0;
		if (index == i) {
			for (auto row: new_cell_counts) {
	        	        op_stream.write(reinterpret_cast<char *>(row.data()),
					elem_size * num_genes);
			}
		} else {
			for (auto row: get<1>(item)) {
                                op_stream.write(reinterpret_cast<char *>(row.data()),
                                        elem_size * num_genes);
                        }
		}
		i++;
        }
}

int process_matrices(fs::path path, fs::path& output_path) {
        DIR *dp;
        struct dirent *dirp;
	fs::path base_path;
	vector<double> rpc;
	vector<tuple<string, vector<vector<double>>>> cell_counts;

        if ((dp = opendir(path.c_str())) == NULL) {
                cout << "Error opening dir\n";
		return -1;
        }

	string cols_path;
        while ((dirp = readdir(dp)) != NULL) {
		if (strcmp(dirp->d_name, ".") != 0 && strcmp(dirp->d_name, "..") != 0) {
			base_path = path;

			string matrix_path = get_path(base_path, dirp->d_name, ".gz");
			string rows_path = get_path(base_path, dirp->d_name, "_rows.txt");
			cols_path = get_path(base_path, dirp->d_name, "_cols.txt");

			vector<vector<double>> matrix_count;
			rpc.push_back(read_files(matrix_path, matrix_count)); // reads per cell
			cell_counts.push_back(make_tuple(dirp->d_name, matrix_count));
                }
	}
	double fraction = min(rpc.at(0), rpc.at(1)) / max(rpc.at(0), rpc.at(1));
	cout << "fraction: " << fraction << endl;
	normalize(cell_counts, fraction, rpc, output_path);
        closedir(dp);
	return 0;
}

int main(int argc, char *argv[]) {
	string s_path = argv[1];
	string s_output_path = argv[2];

	fs::path in_path{s_path};
	fs::path out_path{s_output_path};

	cout << "Starting normalization.." << endl;
	auto s = std::chrono::high_resolution_clock::now();

	int ret = process_matrices(in_path, out_path);

	auto e = std::chrono::high_resolution_clock::now();
	cout << "Time to normalize: " << std::chrono::duration_cast<std::chrono::milliseconds>(e - s).count() << " ms" << endl;
	cout << "exiting.." << endl;

	return 0;
}

