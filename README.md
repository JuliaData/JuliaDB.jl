| Docs | Build | Coverage |
|------|-------|----------|
| [![](https://img.shields.io/badge/docs-latest-blue.svg)](http://juliadb.org/latest/) | [![Build Status](https://travis-ci.org/JuliaComputing/JuliaDB.jl.svg?branch=master)](https://travis-ci.org/JuliaComputing/JuliaDB.jl) | [![codecov](https://codecov.io/gh/JuliaComputing/JuliaDB.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/JuliaComputing/JuliaDB.jl)


# JuliaDB

### JuliaDB is a package for working with large persistent data sets

We recognized the need for an all-Julia, end-to-end tool that can

- **Load multi-dimensional datasets quickly and incrementally.**
- **Index the data and perform filter, aggregate, sort and join operations.**
- **Save results and load them efficiently later.**
- **Use Julia's built-in parallelism to fully utilize any machine or cluster.**

We built JuliaDB to fill this void.

### JuliaDB is built on [Dagger](https://github.com/JuliaParallel/Dagger.jl) and [IndexedTables](https://github.com/JuliaComputing/IndexedTables.jl)

- JuliaDB provides distributed table and array datastructures with convenient functions to load data from CSV. 
- JuliaDB is Julia all the way down. This means queries can be efficiently composed with packages from the entire Julia ecosystem.