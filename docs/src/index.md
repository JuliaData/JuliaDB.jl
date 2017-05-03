```@meta
CurrentModule = JuliaDB
```

# JuliaDB.jl

## Overview

The JuliaDB package provides a distributed table data structure where some of the columns form a sorted index. This structure is equivalent to an N-dimensional sparse array, and follows the array API to the extent possible.
While a table data structure provided by JuliaDB can be used for any kind of array data, it is highly efficient at the storage and querying of data sets whose indices have a natural sorted order, such as time-series data.

The JuliaDB package provides functionality for ingesting data from a variety of data sources, and provides full integration with the rest of the Julia language ecosystem for performing analytics directly on data stored within a JuliaDB table using many Julia processes.

## Installation

```julia
Pkg.clone("https://github.com/JuliaComputing/JuliaDB.jl.git")
```

## Introduction

The data structure (called `DTable`) provided by this package maps tuples of indices to data values.  
Hence, it is similar to a hash table mapping tuples to values, but with a few key differences.
First, the index tuples are stored columnwise, with one vector per index position: there is a vector of first indices, a vector of second indices, and so on.
The index vectors are expected to be homogeneous to allow more efficient storage.
Second, the indices must have a total order, and are stored lexicographically sorted (first by the first index, then by the second index, and so on, left-to-right).
While the indices must have totally-ordered types, the data values can be anything.
Finally, for purposes of many operations an `DTable` acts like an N-dimensional array of its data values, where the number of dimensions is the number of index columns.  A `DTable` implements a distributed memory version of the `IndexedTable` data structure provided by the `IndexedTables.jl` package and re-exported by JuliaDB.
