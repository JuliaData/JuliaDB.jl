# JuliaDB

[![JuliaDB](https://juliarun-ci.s3.amazonaws.com/push/JuliaComputing/JuliaDB/julia_0_5.svg)](https://juliarun-ci.s3.amazonaws.com/push/JuliaComputing/JuliaDB/julia_0_5.log) [![JuliaDB](https://juliarun-ci.s3.amazonaws.com/push/JuliaComputing/JuliaDB/julia_0_6.svg)](https://juliarun-ci.s3.amazonaws.com/push/JuliaComputing/JuliaDB/julia_0_6.log)

JuliaDB is a package for working with large persistent data sets.
Given a set of CSV files, it builds and saves an index that allows the data to be accessed
efficiently in the future.
It also supports an "ingest" mode that converts data to a more efficient binary format.

JuliaDB is based on [Dagger](https://github.com/JuliaParallel/Dagger.jl) and
[IndexedTables](https://github.com/JuliaComputing/IndexedTables.jl), providing a distributed-array-like
data model.
Over time, we hope to expand this to include dense arrays and other Julia array types.
