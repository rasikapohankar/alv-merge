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

int read_files(string matrix_path, string row_file_path,
		unordered_map<string, int> cb_counts,
		unordered_map<string, vector<vector<double>>>& cell_counts,
		io::filtering_ostream& op_matrix_stream, ofstream& out_reads) {
	int num_genes = 52325, //TO-DO: read from _cols file
            count = 0;

	auto start = std::chrono::high_resolution_clock::now();

	// open file containing reads
	ifstream row_file(row_file_path, ios_base::in);
	string s_read;

	// open matrix file
	io::filtering_istream matrix_stream;
	matrix_stream.push(io::gzip_decompressor());
	matrix_stream.push(io::file_source(matrix_path, 
				ios_base::in | ios_base::binary));

	size_t elem_size = sizeof(typename vector<double>::value_type);
	vector<double> num_vector (num_genes, 0.0);
	while (matrix_stream.read(reinterpret_cast<char *>(num_vector.data()),
					elem_size * num_genes)) {
		getline(row_file, s_read);

		if (cb_counts.at(s_read) >= 1) {
			count = cb_counts.at(s_read);
			cb_counts.at(s_read) = count - 1;
		}
		op_matrix_stream.write(reinterpret_cast<char *>(
					num_vector.data()), elem_size * num_genes);
                out_reads << s_read << "_" << count  << "\n";
	}

	row_file.close();
	auto end = std::chrono::high_resolution_clock::now();
	cout << "Time for file: " << matrix_path
				  << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
				  << " ms" << endl;
	return 0;
}


int copy_files(string matrix_path, string row_file_path,
		string col_file_path, unordered_map<string, int>& cb_counts,
		std::ostream& op_file, ofstream& out_rows, ofstream& out_cols) {
	int num_genes = 52325, //TO-DO: read from _cols file
	    count = 0;

	ifstream row_file(row_file_path, ios_base::in);
	ifstream col_file(col_file_path, ios_base::in);
	string s_read, s_col;

	auto s = std::chrono::high_resolution_clock::now();
	// open input matrix file
	io::filtering_istream matrix_stream;
	matrix_stream.push(io::file_source(matrix_path, 
				ios_base::in | ios_base::binary));

	io::copy(matrix_stream, op_file, 20480);

	int num_cols = 0;
	while (getline(col_file, s_col)) {
		num_cols++;
	}
	cout << "num cols in " << col_file_path << ": " << num_cols << endl;

	auto ls = std::chrono::high_resolution_clock::now();
	while (getline(row_file, s_read)) {
		if (cb_counts.at(s_read) >= 1) {
			count = cb_counts.at(s_read);
			cb_counts.at(s_read) = count - 1;
                }
		out_rows << s_read << "_" << count << "\n";
	}
	auto le = std::chrono::high_resolution_clock::now();
//	cout << "Time for loop: " << std::chrono::duration_cast<std::chrono::milliseconds>(le - ls).count() << " ms" << endl;

	row_file.close();
	auto e = std::chrono::high_resolution_clock::now();
	cout << "Time for file: " << std::chrono::duration_cast<std::chrono::milliseconds>(e - s).count() << " ms" << endl;
	return 0;
}

int concat_matrix(boost::filesystem::path path, boost::filesystem::path& output_path,
		  unordered_map<string, int>& cb_counts, int num_matrices) {
        DIR *dp;
        struct dirent *dirp;
	boost::filesystem::path base_path;
	int count = 0;
	unordered_map<string, vector<vector<double>>> cell_counts;

        if ((dp = opendir(path.c_str())) == NULL) {
                cout << "Error opening dir\n";
		return -1;
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

			read_files(matrix_path, rows_file, cb_counts, cell_counts, op_matrix_stream, out_rows);
			//copy_files(matrix_path, rows_file, cols_file, cb_counts, op_file, out_rows, out_cols);
			count++;
                }
		if (count == num_matrices)
			break;
	}
	auto cols_path = output_path / "out_cols.txt";
        out_cols.open(cols_path.string());
        ifstream col_file(cols_file, ios_base::in);
	cout << "f: " << cols_file << endl;
        string gene;
        while (getline(col_file, gene)) {
//		cout << gene << endl;
                out_cols << gene << "\n";
        }
        out_cols.close();

	cout << "count: " << count << endl;
        closedir(dp);
	out_rows.close();
	op_matrix.close();
	cout << "closed dir " << endl;
	cout << "closed out reads file" << endl;
	return 0;
}

int main(int argc, char *argv[]) {
	string s_path = argv[1];
	string s_output_path = argv[2];
	int num_matrices = std::stoi(argv[3]);

	boost::filesystem::path in_path{s_path};
	boost::filesystem::path out_path{s_output_path};

	cout << "Getting cb counts" << endl;
        auto start = std::chrono::high_resolution_clock::now();
        unordered_map<string, int> cb_counts = get_cb_counts(in_path, num_matrices);
        auto end = std::chrono::high_resolution_clock::now();
        cout << "CB counts time " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " ms" << endl;

	cout << "Starting concatenation.." << endl;
	auto s = std::chrono::high_resolution_clock::now();
	int ret = concat_matrix(in_path, out_path, cb_counts, num_matrices);
	auto e = std::chrono::high_resolution_clock::now();
	cout << "Time to concatenate: " << std::chrono::duration_cast<std::chrono::milliseconds>(e - s).count() << " ms" << endl;
	cout << "exiting.." << endl;

	return 0;
}

