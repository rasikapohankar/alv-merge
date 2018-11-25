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
namespace io=boost::iostreams; 
namespace fs=boost::filesystem;

int read_row_file(string path, unordered_map<string, int>& map) {
        string line;
        ifstream infile(path);

        while (getline(infile, line)) {
                if (map.count(line) > 0) {
                        map.at(line) = map.at(line) + 1;
                } else {
                        map.insert({line, 1});
                }
        }
        infile.close();
        return 0;
}

string get_path(fs::path base, string dir, string suffix) {
        base = base / dir / dir;
        base += suffix;
        return base.string();
}

unordered_map<string, int> get_cb_counts(fs::path path) {
	vector<string> files = vector<string>();
	DIR *dp;
	struct dirent *dirp;
	unordered_map<string, int> map;
	fs::path base_path;
	if ((dp = opendir(path.c_str())) == NULL) {
		cout << "Error opening dir\n";
	//	return;
	}

	while ((dirp = readdir(dp)) != NULL) {
		if (strcmp(dirp->d_name, ".") != 0 && 
			strcmp(dirp->d_name, "..") != 0) {
			base_path = path;
			string file_path = get_path(base_path, dirp->d_name, "_rows.txt");
			read_row_file(file_path, map);
		}
	}
	closedir(dp);
	return map;
}

int read_files(string matrix_path, vector<vector<double>>& cell_counts)
{
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
        cout << "Time for file: " << matrix_path << " "
             << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
             << " ms" << endl;
        return sum;
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

void normalize(unordered_map<string, vector<vector<double>>> cell_counts, int max,
		fs::path output_path) {
	size_t elem_size = sizeof(typename vector<double>::value_type);
	int num_genes = 52325;
        for (auto item : cell_counts) {
                for (auto row: item.second) {
                        std::transform(row.begin(), row.end(), row.begin(),
                        [&max](double c) -> double { return c / max; });
                }       
                auto matrix_out_path = output_path / item.first;
                matrix_out_path += ".gz";

		io::filtering_ostream op_stream;
		auto path = output_path / item.first;
		path += ".gz";
	        open_stream(path, op_stream);
                op_stream.write(reinterpret_cast<char *>((item.second).data()),
					elem_size * num_genes * (item.second).size());
        }
}

int concat_matrix(fs::path path, fs::path& output_path,
		  unordered_map<string, int>& cb_counts) {
        DIR *dp;
        struct dirent *dirp;
	fs::path base_path;
	double sum = 0.0,
	       max = 0.0;
	unordered_map<string, vector<vector<double>>> cell_counts;

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
			sum = read_files(matrix_path, matrix_count);
			cout << "matrix 1 sum " << sum << endl;
			cell_counts.insert({dirp->d_name, matrix_count});
			if (sum > max)
				max = sum;
                }
	}
	normalize(cell_counts, max, output_path);
        closedir(dp);
	return 0;
}

int main(int argc, char *argv[]) {
	string s_path = argv[1];
	string s_output_path = argv[2];

	fs::path in_path{s_path};
	fs::path out_path{s_output_path};

	cout << "Getting cb counts" << endl;
        auto start = std::chrono::high_resolution_clock::now();
        unordered_map<string, int> cb_counts = get_cb_counts(in_path);
        auto end = std::chrono::high_resolution_clock::now();
        cout << "CB counts time " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " ms" << endl;

	cout << "Starting concatenation.." << endl;
	auto s = std::chrono::high_resolution_clock::now();
	int ret = concat_matrix(in_path, out_path, cb_counts);
	auto e = std::chrono::high_resolution_clock::now();
	cout << "Time to concatenate: " << std::chrono::duration_cast<std::chrono::milliseconds>(e - s).count() << " ms" << endl;
	cout << "exiting.." << endl;

	return 0;
}

