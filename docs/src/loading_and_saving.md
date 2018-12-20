```@setup loadsave
using Pkg
Pkg.add("RDatasets")
```

# Loading and Saving Data

## Loading Data From CSV

Loading a CSV file (or multiple files) into one of JuliaDB's [Data Structures](@ref) is accomplished via the [`loadtable`](@ref) and [`loadndsparse`](@ref) functions.

```@example loadsave
using JuliaDB, DelimitedFiles

x = rand(10, 2)
writedlm("temp.csv", x, ',')

t = loadtable("temp.csv")
```

## Converting From Other Data Structures

```@example loadsave
using JuliaDB, RDatasets

df = dataset("datasets", "iris")  # load data as DataFrame

table(df)
```

## Save Table into Binary Format

A table can be saved to disk for fast, efficient reloading via the [`save`](@ref) function.

## Load Table from Binary Format

Tables can be loaded efficiently via [`load`](@ref).