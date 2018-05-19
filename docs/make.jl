using Documenter, JuliaDB, IndexedTables, Dagger, TextParse, Glob

load_dir(x) = map(file -> joinpath("lib", x, file), readdir(joinpath(Base.source_dir(), "src", "lib", x)))

makedocs(
   modules = [JuliaDB, IndexedTables, NamedTuples],
   clean = false,
   format = [:html],#, :latex],
   sitename = "JuliaDB",
   pages = Any[
       "Home" => "index.md",
       "API Reference" => "api/index.md",
       "Data Structures" => "api/datastructures.md",
       "Selection" => "api/selection.md",
       "Aggregation" => "api/aggregation.md",
       "Joins" => "api/joins.md",
       "Reshaping" => "api/reshaping.md",
       "Loading and Saving" => "api/io.md",
       "Plotting" => "api/plotting.md",
       "OnlineStats Integration" => "manual/onlinestats.md",
       "Out-of-core functionality" => "manual/out-of-core.md",
       "Feature Extraction" => "manual/ml.md",
       "Tutorial" => "manual/tutorial.md",
   ],
   assets = ["assets/custom.css", "assets/custom.js"]
)

deploydocs(
    repo = "github.com/JuliaComputing/JuliaDB.jl.git",
    target = "build",
    julia = "0.6",
    osname = "linux",
    deps = nothing,
    make = nothing,
)
