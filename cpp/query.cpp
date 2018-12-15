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
#include <boost/program_options.hpp>
#include <chrono>
#include "seekgzip/seekgzip.c"

using namespace std;
namespace po = boost::program_options;
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


int main(int argc, char **argv) {

	string s_path, matrix_name;
        int row, col, build_index;

	/*********** Option handling  ****************/
	po::options_description desc("Allowed options");
	try {
		desc.add_options()
			("help", "produce help message")
			("src", po::value<string>(&s_path)->required(),
				"Path of directory containing the concatenated matrix")
			("matrix,m", po::value<string>(&matrix_name)->required(),
				"Matrix name to query from, such as \"E18_20160930_Neurons_Sample_12_quants_mat\"")
			("row,r", po::value<int>(&row)->required(),
				"Row number of matrix to be queried")
			("col,c", po::value<int>(&col)->required(),
				"Column number of matrix to be queried")
			("build,b", po:: value<int>(&build_index)->default_value(0),
				"Flag specifying whether to build index, 1 is yes, 0 no. Default value value is 0");

		po::variables_map vm;
		po::store(po::parse_command_line(argc, argv, desc), vm);

		if (vm.count("help")) {
			cout << "trying to show help" << endl << endl;
			cout << desc << endl;
			return 0;
		}
		po::notify(vm);
	} catch (boost::program_options::required_option& e) {
		cout << "Error: " << e.what() << endl;
		cout << desc << endl;
		return 1;
	}
	/*********** Option handling  ****************/

	fs::path in_path{s_path};
	if (!fs::exists(in_path)) {
		cout << "That path '" << s_path << "' does not seem to exist!" << endl;
		return 1;
	}
	vector<tuple<string, int, int>> counts;
        unordered_map<string, int> matrix_index;
	get_metadata(in_path, counts, matrix_index);

	auto start = std::chrono::high_resolution_clock::now();
	long int offset = get_offset(matrix_index, counts, matrix_name, row, col);
	auto end = std::chrono::high_resolution_clock::now();
	cout << "offset " << offset << endl;
	cout << "Time to calculate offset "
	     << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
	     << " ms" << endl;

	auto path = in_path / "out_matrix.gz";
	const char *matrix_path = (path.string()).c_str();
	cout << "path for index: " << matrix_path << endl;
	if (build_index == 1) {
		auto start = std::chrono::high_resolution_clock::now();
		int ret = seekgzip_build(matrix_path);
	        if (ret != 0) {
		    cout << "Error building index, exiting.." << endl;
        	    return 1;
	        }
		auto end = std::chrono::high_resolution_clock::now();
		cout << "Time to build index "
		     << std::chrono::duration_cast<chrono::milliseconds>(end - start).count()
		     << " ms" << endl;
	}

	auto q_s = std::chrono::high_resolution_clock::now();
	double value = query_matrix(matrix_path, offset);
	auto q_e = std::chrono::high_resolution_clock::now();
	cout << "value in " << matrix_name << " at " 
	     << row << "," << col << " is " << value << endl;

	cout << "Time to query "
	     << std::chrono::duration_cast<std::chrono::milliseconds>(q_e - q_s).count()
	     << " ms" << endl;

	cout << "exiting query.." << endl;
	return 0;
}

