| Documentation | Master Build | Test Coverage |
|---------------|--------------|---------------|
| [![](https://img.shields.io/badge/docs-latest-blue.svg)](http://juliadb.org/latest/api/) | [![Build Status](https://travis-ci.org/JuliaComputing/JuliaDB.jl.svg?branch=master)](https://travis-ci.org/JuliaComputing/JuliaDB.jl) | [![codecov](https://codecov.io/gh/JuliaComputing/JuliaDB.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/JuliaComputing/JuliaDB.jl)


# JuliaDB

### JuliaDB is a package for working with large persistent data sets

Given a directory of CSV files, JuliaDB builds and saves an index that allows the data to be accessed efficiently in the future.

### JuliaDB is built on [Dagger](https://github.com/JuliaParallel/Dagger.jl) and [IndexedTables](https://github.com/JuliaComputing/IndexedTables.jl)

This allows JuliaDB to use a a distributed-array-like data model.
