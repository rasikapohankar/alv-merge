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
* Open all rows files and create map of CB index counts
* Open each zipped matrix and corresponding row file
  * If CB from row file has count 1 in the above map, write the float values 
    to output file
  * If the count > 1, store in a separate map as read\_string -> [[float values]]
* Write the map to the output file, appending '\_number' to read string

This ensures that all unique CB row values will be at the top in the output file
and all repeated ones will be together at the end of the file.

Compile C++ code:
g++ -std=c++11 concatenate\_matrices.cpp -lz /usr/include/boost/stage/lib/libboost\_iostreams.so -o concatenate\_matrices

g++ -std=c++11 concatenate\_matrices.cpp -lboost\_iostreams -o concatenate\_matrices

For self-reference:
* Download the boost tar, untar it
* Run bootstrap.sh for compiling iostream lib
* Use above command to create out file
* Update LD\_LIBRARY\_PATH in bashrc to /boost/stage/lib folder so that the linker is able to find the library while executing
