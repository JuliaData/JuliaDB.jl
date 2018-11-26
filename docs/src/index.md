# Overview

**JuliaDB is a package for working with persistent data sets.**

We recognized the need for an all-Julia, end-to-end tool that can

1. Load multi-dimensional datasets quickly and incrementally.
2. Index the data and perform filter, aggregate, sort and join operations.
3. Save results and load them efficiently later.
4. Readily use Julia's built-in [parallelism](https://docs.julialang.org/en/stable/manual/parallel-computing/) to fully utilize any machine or cluster.

We built JuliaDB to fill this void.

JuliaDB provides distributed table and array datastructures with convenient functions to load data from CSV. JuliaDB is Julia all the way down. This means queries can be composed with Julia code that may use a vast ecosystem of packages.


## Quickstart

```julia
# Install JuliaDB
using Pkg
pkg"add JuliaDB"

# Load the JuliaDB Package
using JuliaDB

# Create a table where the first column is the "primary key"
t = table(rand(Bool, 10), rand(10), pkey=1)
```

## Parallelism

- The parallel/distributed features of JuliaDB are available by starting Julia with worker processes (`julia -p N`) or by running

```julia
addprocs(N)
```

before running

```julia
using JuliaDB
```

- Multiple processes may not be benificial for datasets with less than a few million rows. Communication costs are eliminated on a single process, but of course you will only be using a single CPU.

## Additional Resources

- [#juliadb Channel in the JuliaLang Slack](https://julialang.slack.com/messages/C86LDBEBD/)
- [JuliaLang Discourse](https://discourse.julialang.org)

