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
#include <boost/iostreams/categories.hpp>
#include <chrono>
#include "seekgzip/seekgzip.c"

using namespace std;
namespace io=boost::iostreams; 
namespace fs=boost::filesystem;

string get_path(fs::path base, string dir, string suffix) {
        base = base / dir / dir;
        base += suffix;
        return base.string();
}

tuple<string, int, int> split(string line) {
	size_t pos = 0;
	vector<string> values;
	while ((pos = line.find(",")) != string::npos) {
		string token = line.substr(0, pos);
		line.erase(0, pos + 1);
		values.push_back(token);
	}
	values.push_back(line);
	return make_tuple(values.at(0), stoi(values.at(1)), stoi(values.at(2)));
}

void get_metadata(fs::path path, vector<tuple<string, int, int>>& counts,
			unordered_map<string, int>& matrix_index) {
        auto counts_path = path / "out_counts.txt";
        ifstream counts_file(counts_path.string(), ios_base::in);
        string line;
        int index = 0;
        while (getline(counts_file, line)) {
                tuple<string, int, int> tup = split(line);
                counts.push_back(tup);
                matrix_index.insert({get<0>(tup), index});
                index++;
        }
}

long int get_offset(unordered_map<string, int> matrix_index,
		vector<tuple<string, int, int>> counts, 
		string matrix_name, int row, int col) {
	cout << "matrix name: " << matrix_name << endl;
	cout << "getting offset.." << endl;

	int index = matrix_index.at(matrix_name);
	tuple<string, int, int> t = counts.at(index);

	int c = 0;
	size_t elem_size = sizeof(typename vector<double>::value_type);
	long int offset = 0;
	while (c < index) {
		tuple<string, int, int> tup = counts.at(c);
		offset += get<1>(tup) * get<2>(tup) * elem_size;
		c++;
	}
	offset += (row - 1) * elem_size + (col - 1) * elem_size;
	return offset;
}

double query_matrix(const char *path, off_t offset) {
	double value;

	seekgzip_t* zs = seekgzip_open(path, NULL);
	if (zs == NULL) {
		fprintf(stderr, "ERROR: Failed to open the index file.\n");
		return -1;
	}
	seekgzip_seek(zs, offset);
	int len = seekgzip_read(zs, &value, sizeof(double));
	cout << "read bytes: " << len << endl;
	cout << "Exiting from query_matrix" << endl;
	return value;
}


int main(int argc, char *argv[]) {

	string s_path = argv[1],
	       matrix_name = argv[2];
	int row = stoi(argv[3]),
	    col = stoi(argv[4]),
	    build_index = stoi(argv[5]);

	fs::path in_path{s_path};

	vector<tuple<string, int, int>> counts;
        unordered_map<string, int> matrix_index;
	get_metadata(in_path, counts, matrix_index);

	long int offset = get_offset(matrix_index, counts, matrix_name, row, col);
	cout << "offset " << offset << endl;

	auto path = in_path / "out_matrix.gz";
	const char *matrix_path = (path.string()).c_str();
	cout << "path for index: " << matrix_path << endl;
	if (build_index == 1) {
		int ret = seekgzip_build(matrix_path);
	        if (ret != 0) {
		    cout << "Error building index, exiting.." << endl;
        	    return 1;
	        }
	}

	double value = query_matrix(matrix_path, offset);
	cout << "value in " << matrix_name << " at " 
	     << row << "," << col << " is " << value << endl;

	cout << "exiting query.." << endl;
	return 0;
}

