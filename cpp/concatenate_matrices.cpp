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

unordered_map<string, int> get_cb_counts(boost::filesystem::path path) {
	vector<string> files = vector<string>();
	DIR *dp;
	struct dirent *dirp;
	unordered_map<string, int> map;
	boost::filesystem::path base_path;

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
			cout << base_path.string() << endl;
			int count = read_row_file(base_path.string(), map);
		}
	}
	closedir(dp);
	return map;
}

int read_files(string matrix_path,
		string row_file_path,
		unordered_map<string, int>& cb_counts,
		std::ostream& op_file,
                ofstream& out_reads
	      ) {
	time_t start_t, end_t, ws, we;
	int num_genes = 52325; //TO-DO: read from _cols file
	int count = 0;

	ifstream row_file(row_file_path, ios_base::in);
	string s_read;

	time(&start_t);
	// open input matrix file
	boost::iostreams::filtering_istream matrix_stream;
	matrix_stream.push(boost::iostreams::file_source(matrix_path,
							 ios_base::in | ios_base::binary));

	boost::iostreams::copy(matrix_stream, op_file, 20480);	

	time_t ls, le;
	time(&ls);
	while (getline(row_file, s_read)) {
		if (cb_counts.at(s_read) >= 1) {
			count = cb_counts.at(s_read);
			cb_counts.at(s_read) = count - 1;
                }
		out_reads << s_read << "_" << count << "\n";
	}
	time(&le);
	cout << "time for loop " << le - ls << endl;

	row_file.close();
	time(&end_t);
	cout << "Time for file " << matrix_path << ": " << end_t - start_t << " sec" << endl;
	return 0;
}

int concat_matrix(boost::filesystem::path path,
		  boost::filesystem::path& output_path, 
		  unordered_map<string, int>& cb_counts
		) {
        DIR *dp;
        struct dirent *dirp;
	boost::filesystem::path base_path;

	cout << "dir in concat matrix " << path.string() << endl;
	
        if ((dp = opendir(path.c_str())) == NULL) {
                cout << "Error opening dir\n";
        //      return;
        }

	// open output matrix file
        auto matrix_out_path = output_path / "out_matrix.gz";
	std::filebuf op_matrix;
	op_matrix.open(matrix_out_path.string(), std::ios::out);
	std::ostream op_file(&op_matrix);

	// open output reads file
        ofstream out_reads;
        auto reads_path = output_path / "out_reads.txt";
        out_reads.open(reads_path.string());

        while ((dirp = readdir(dp)) != NULL) {
		if (strcmp(dirp->d_name, ".") != 0 && strcmp(dirp->d_name, "..") != 0) {
			base_path = path;
			base_path = base_path / dirp->d_name / dirp->d_name;
			base_path += ".gz";
			string matrix_path = base_path.string();

			base_path.remove_leaf();
			base_path = base_path / dirp->d_name;
			base_path += "_rows.txt";
			string file_path = base_path.string();

			read_files(matrix_path, file_path, cb_counts, op_file, out_reads);
                }
	}
        closedir(dp);
	out_reads.close();
	op_matrix.close();
	cout << "closed dir " << endl;
	cout << "closed out reads file" << endl;
	return 0;
}

int main(int argc, char *argv[]) {
	string s_path = argv[1];
	string s_output_path = argv[2];

	boost::filesystem::path in_path{s_path};
	boost::filesystem::path out_path{s_output_path};
	
	time_t start, end;

	cout << "Getting cb counts" << endl;
	time (&start);
	unordered_map<string, int> cb_counts = get_cb_counts(in_path);
	time (&end);
	cout << "Got cb counts" << endl;
	cout << "Time " << end - start << " sec" << endl;

	cout << "Starting concatenation.." << endl;
	time(&start);
	cout << concat_matrix(in_path, out_path, cb_counts) << endl;
	cout << "returned from concat_matrix" << endl;
	time(&end);
	cout << "Time to concatenate " << end - start << " sec" << endl;
	cout << "exiting.." << endl;

	for (auto &x: cb_counts) {
		if (x.second > 0)
	        	cout << x.first << ":" << x.second << endl;
       	}


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
