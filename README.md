# alv-merge
Concatenate and compress Alevin matrices (CSE 524)

### Steps to run
* Compile: g++ -std=c++11 -O3 concatenate\_matrices.cpp -lboost\_iostreams -lboost\_system -lboost\_filesystem -o concatenate\_matrices
* Download matrices from the GDrive and run make\_dirs.sh to create directories
	* sh make\_dir.sh `source dir` `destination dir`
* This will create dirs of the form:
        * ./alevin\_matrices/`<id>`\_quants\_mat/`<id>`\_quants\_mat.gz
        * ./alevin\_matrices/`<id>`\_quants\_mat/`<id>`\_quants\_mat\_rows.txt
        * ./alevin\_matrices/`<id>`\_quants\_mat/`<id>`\_quants\_mat\_cols.txt
* Execute: ./concatenate\_matrices `source dir` `output dir`
		   

### Current implementation:
* cpp/concatenate\_matrices.cpp: Creates a concatednated matrix stored as compressed binary, and two output text files with all the
row and column names.

  * Approach:
    * Create and open output matrix file in binary mode
    * Copy each input matrix to the output matrix, without reading it since the values in the matrix are not processed
    * Buffer size used for copying is 20480 MB.

  * Runtime:
    * The time required to concatenate all matrices (69) using this approach is 4 seconds.

  * How to run:
    * Compile: `g++ -std=c++11 -O3 concatenate_matrices.cpp -lboost iostreams -lboost system -lboost filesystem -o concatenate_matrices`
    * Run: `./concatenate_matrices source dir destination dir`

* cpp/normalize\_matrices.cpp:
  * Approach:
    * Normalize two matrices at a time, to make them of the same depth. 
    * It is done by finding the reads per cell (rpc) for each matrix and then the fraction of reads to be kept, Fraction of reads = min(rpc\_1, rpc\_2) / max(rpc\_1, rpc\_2).
    * Matrix with smaller rpc is divided by fraction of reads to be kept.

  * Runtime:
    * Normalization takes average 40 seconds to complete, since both the matrices have to be read.

  * How to run:
    * Compile: `g++ -std=c++11 -O3 normalize_matrices.cpp -lboost iostreams -lboost system -lboost filesystem -o normalize_matrices`
    * Run: `./normalize matrices source dir destination dir`, `source dir` should have the 2 matrices to be normalized, with the same directory struc-
ture as used for concatenation. The two output matrices will be created in `destination dir`.

* cpp/query.cpp:
  * Approach:
    * Query the concatenated matrix for a specific value. The user will provide the matrix name, row and column number to query from.
    * The program returns the value present in that cell.
    * In order to seek on the concatenated matrix, we use [seekgzip] ().
    * The utility builds an index on the compressed binary matrix, and allows random seek.

  * Runtime:
    * The offset calculation takes less than 1 ms. The time taken to build the index on concatenated 69 matrices is 12 s. 
      The time for query after the index is built is: 0.235 ms. The last matrix processed during concatenation was E18 20160930 Neurons Sample 36 quants mat. 
      The query for its last cell (12082, 52325) takes 0.316 s.

  * How to run:
    * Compile: `g++ -std=c++11 -O3 query.cpp -o query -lz -lboost iostreams -lboost system -lboost filesystem`
    * Run: `./query –src <string> -m <string> -r <int> -c <int> [-b <int>]`
    * Allowed options:
	  --help                  produce help message
	  --src arg               Path of directory containing the concatenated matrix
	  -m [ --matrix ] arg     Matrix name to query from, such as
        	                  "E18_20160930_Neurons_Sample_12_quants_mat"
	  -r [ --row ] arg        Row number of matrix to be queried
	  -c [ --col ] arg        Column number of matrix to be queried
	  -b [ --build ] arg (=0) Flag specifying whether to build index, 1 is yes, 0
                          no. Default value value is 0


For self-reference (installing boost libraries):
* Download the boost tar, untar it
* Run bootstrap.sh for compiling iostream lib
* Use above command to create out file
* Update LD\_LIBRARY\_PATH in bashrc to /boost/stage/lib folder so that the linker is able to find the library while executing
