# alv-merge
Concatenate and compress Alevin matrices (CSE 524)

Current implementation:
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
