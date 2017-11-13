```@meta
CurrentModule = JuliaDB
```
# JuliaDB
![](https://juliacomputing.com/assets/img/new/4dots.svg)

## Overview

*JuliaDB is a package for working with persistent data sets.*

We recognized the need for an all-Julia, end-to-end tool that can

1. Load multi-dimensional datasets quickly and incrementally.
2. Index the data and perform filter, aggregate, sort and join operations.
3. Save results and load them efficiently later.
4. Readily use Julia's built-in [parallelism](https://docs.julialang.org/en/stable/manual/parallel-computing/) to fully utilize any machine or cluster.

We built JuliaDB to fill this void.

JuliaDB provides distributed table and array datastructures with convenient functions to load data from CSV. JuliaDB is Julia all the way down. This means queries can be composed with Julia code that may use a vast ecosystem of packages.

## Getting started

JuliaDB works on Julia 0.6. To install it, run:

```julia
Pkg.add("JuliaDB")
```

To use JuliaDB, you may start Julia with a few worker processes (`julia -p N`) or, alternatively, run `addprocs(N)` before running

```@repl sampledata
using JuliaDB
```

Multiple processes may not be benificial for datasets with less than a few million rows. Communication costs are eliminated on a single process, but of course you will be using a single CPU.

## Resources

- [API Reference](api/index.html)
- [Tutorial](tutorial.html)
- [Issue tracker](https://github.com/JuliaComputing/JuliaDB.jl/issues)
- [Source code](https://github.com/JuliaComputing/JuliaDB.jl)
- [Slack channel]()

