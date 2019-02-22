var documenterSearchIndex = {"docs": [

{
    "location": "#",
    "page": "Overview",
    "title": "Overview",
    "category": "page",
    "text": "<center>\n<img src=\"https://user-images.githubusercontent.com/25916/36773410-843e61b0-1c7f-11e8-818b-3edb08da8f41.png\" width=200>\n</center>"
},

{
    "location": "#Overview-1",
    "page": "Overview",
    "title": "Overview",
    "category": "section",
    "text": "JuliaDB is a package for working with persistent data sets.We recognized the need for an all-Julia, end-to-end tool that canLoad multi-dimensional datasets quickly and incrementally.\nIndex the data and perform filter, aggregate, sort and join operations.\nSave results and load them efficiently later.\nReadily use Julia\'s built-in parallelism to fully utilize any machine or cluster.We built JuliaDB to fill this void.JuliaDB provides distributed table and array datastructures with convenient functions to load data from CSV. JuliaDB is Julia all the way down. This means queries can be composed with Julia code that may use a vast ecosystem of packages."
},

{
    "location": "#Quickstart-1",
    "page": "Overview",
    "title": "Quickstart",
    "category": "section",
    "text": "using Pkg\nPkg.add(\"JuliaDB\")\n\nusing JuliaDB\n\n# Create a table where the first column is the \"primary key\"\nt = table(rand(Bool, 10), rand(10), pkey=1)"
},

{
    "location": "#Parallelism-1",
    "page": "Overview",
    "title": "Parallelism",
    "category": "section",
    "text": "The parallel/distributed features of JuliaDB are available by either:Starting Julia with N workers: julia -p N\nCalling addprocs(N) before using JuliaDBnote: Note\nMultiple processes may not be benificial for datasets with less than a few million rows."
},

{
    "location": "#Additional-Resources-1",
    "page": "Overview",
    "title": "Additional Resources",
    "category": "section",
    "text": "#juliadb Channel in the JuliaLang Slack\nJuliaLang Discourse\nIssue Tracker"
},

{
    "location": "basics/#",
    "page": "Basics",
    "title": "Basics",
    "category": "page",
    "text": "using JuliaDB"
},

{
    "location": "basics/#Basics-1",
    "page": "Basics",
    "title": "Basics",
    "category": "section",
    "text": "JuliaDB offers two main data structures as well as distributed counterparts.  This allows you to easily scale up an analysis, as operations that work on non-distributed tables  either work out of the box or are easy to transition for distributed tables.Here is a high level overview of tables in JuliaDB:Tables store data in columns.\nTables are typed.\nChanging a table in some way therefore requires returning a new table (underlying data is not copied).\nJuliaDB has few mutating operations because a new table is necessary in most cases."
},

{
    "location": "basics/#Data-for-examples:-1",
    "page": "Basics",
    "title": "Data for examples:",
    "category": "section",
    "text": "x = 1:10\ny = vcat(fill(\'a\', 4), fill(\'b\', 6))\nz = randn(10);"
},

{
    "location": "basics/#[IndexedTable](@ref)-1",
    "page": "Basics",
    "title": "IndexedTable",
    "category": "section",
    "text": "An IndexedTable is wrapper around a (named) tuple of Vectors, but it behaves like a Vector of (named) tuples.  You can choose to sort the table by any number of primary  keys (in this case columns :x and :y).An IndexedTable is created with data in Julia via the table function or with  data on disk via the loadtable function.t = table((x=x, y=y, z=z); pkey = [:x, :y])\nt[1]\nt[end]"
},

{
    "location": "basics/#[NDSparse](@ref)-1",
    "page": "Basics",
    "title": "NDSparse",
    "category": "section",
    "text": "An NDSparse has a similar underlying structure to IndexedTable, but it behaves like a sparse array with arbitrary indices.  The keys of an NDSparse are sorted, much like the primary keys of an IndexedTable.An NDSparse is created with data in Julia via the ndsparse function or with  data on disk via the loadndsparse function.nd = ndsparse((x=x, y=y), (z=z,))\nnd[1, \'a\']\nnd[10, \'j\'].z\nnd[1, :]"
},

{
    "location": "basics/#Selectors-1",
    "page": "Basics",
    "title": "Selectors",
    "category": "section",
    "text": "JuliaDB has a variety of ways to select columns.  These selection methods get used across many JuliaDB\'s functions: select, reduce, groupreduce,  groupby, join, pushcol, reindex, and more.To demonstrate selection, we\'ll use the select function.  A selection can be any of the following types:Integer – returns the column at this position.\nSymbol – returns the column with this name.\nPair{Selection => Function} – selects and maps a function over the selection, returns the result.\nAbstractArray – returns the array itself. This must be the same length as the table.\nTuple of Selection – returns a table containing a column for every selector in the tuple.\nRegex – returns the columns with names that match the regular expression.\nType – returns columns with elements of the given type.\nNot(Selection) – returns columns that are not included in the selection.\nBetween(first, last) – returns columns between first and last.\nKeys() – return the primary key columns.t = table(1:10, randn(10), rand(Bool, 10); names = [:x, :y, :z])"
},

{
    "location": "basics/#select-the-:x-vector-1",
    "page": "Basics",
    "title": "select the :x vector",
    "category": "section",
    "text": "select(t, 1)\nselect(t, :x)"
},

{
    "location": "basics/#map-a-function-to-the-:y-vector-1",
    "page": "Basics",
    "title": "map a function to the :y vector",
    "category": "section",
    "text": "select(t, 2 => abs)\nselect(t, :y => x -> x > 0 ? x : -x)"
},

{
    "location": "basics/#select-the-table-of-:x-and-:z-1",
    "page": "Basics",
    "title": "select the table of :x and :z",
    "category": "section",
    "text": "select(t, (:x, :z))\nselect(t, r\"(x|z)\")"
},

{
    "location": "basics/#map-a-function-to-the-table-of-:x-and-:y-1",
    "page": "Basics",
    "title": "map a function to the table of :x and :y",
    "category": "section",
    "text": "select(t, (:x, :y) => row -> row[1] + row[2])\nselect(t, (1, :y) => row -> row.x + row.y)"
},

{
    "location": "basics/#select-columns-that-are-subtypes-of-Integer-1",
    "page": "Basics",
    "title": "select columns that are subtypes of Integer",
    "category": "section",
    "text": "select(t, Integer)"
},

{
    "location": "basics/#select-columns-that-are-not-subtypes-of-Integer-1",
    "page": "Basics",
    "title": "select columns that are not subtypes of Integer",
    "category": "section",
    "text": "select(t, Not(Integer))"
},

{
    "location": "basics/#Loading-and-Saving-1",
    "page": "Basics",
    "title": "Loading and Saving",
    "category": "section",
    "text": "using Pkg\nPkg.add(\"RDatasets\")"
},

{
    "location": "basics/#Loading-Data-From-CSV-1",
    "page": "Basics",
    "title": "Loading Data From CSV",
    "category": "section",
    "text": "Loading a CSV file (or multiple files) into one of JuliaDB\'s tabular data structures is accomplished via the loadtable and loadndsparse functions.  using JuliaDB, DelimitedFiles\n\nx = rand(10, 2)\nwritedlm(\"temp.csv\", x, \',\')\n\nt = loadtable(\"temp.csv\")note: Note\nloadtable and loadndsparse use Missing to represent missing values.  To load a CSV that instead uses DataValue, see CSVFiles.jl.  For more information on missing value representations, see Missing Values."
},

{
    "location": "basics/#Converting-From-Other-Data-Structures-1",
    "page": "Basics",
    "title": "Converting From Other Data Structures",
    "category": "section",
    "text": "using JuliaDB, RDatasets\n\ndf = dataset(\"datasets\", \"iris\")  # load data as DataFrame\n\ntable(df)  # Convert DataFrame to IndexedTable"
},

{
    "location": "basics/#Save-Table-into-Binary-Format-1",
    "page": "Basics",
    "title": "Save Table into Binary Format",
    "category": "section",
    "text": "A table can be saved to disk (for fast, efficient reloading) via the save function."
},

{
    "location": "basics/#Load-Table-from-Binary-Format-1",
    "page": "Basics",
    "title": "Load Table from Binary Format",
    "category": "section",
    "text": "Tables that have been save-ed can be loaded efficiently via load."
},

{
    "location": "operations/#",
    "page": "Table Operations",
    "title": "Table Operations",
    "category": "page",
    "text": "using JuliaDB, OnlineStats"
},

{
    "location": "operations/#Table-Operations-1",
    "page": "Table Operations",
    "title": "Table Operations",
    "category": "section",
    "text": "Pages = [\"operations.md\"]\nDepth = 2"
},

{
    "location": "operations/#Column-Operations-1",
    "page": "Table Operations",
    "title": "Column Operations",
    "category": "section",
    "text": "setcol\npushcol\npopcol\ninsertcol\ninsertcolafter\ninsertcolbefore\nrenamecol"
},

{
    "location": "operations/#[filter](@ref)-1",
    "page": "Table Operations",
    "title": "filter",
    "category": "section",
    "text": ""
},

{
    "location": "operations/#[flatten](@ref)-1",
    "page": "Table Operations",
    "title": "flatten",
    "category": "section",
    "text": ""
},

{
    "location": "operations/#[groupby](@ref)-1",
    "page": "Table Operations",
    "title": "groupby",
    "category": "section",
    "text": ""
},

{
    "location": "operations/#[reduce](@ref)-and-[groupreduce](@ref)-1",
    "page": "Table Operations",
    "title": "reduce and groupreduce",
    "category": "section",
    "text": ""
},

{
    "location": "operations/#[pushcol](@ref)-1",
    "page": "Table Operations",
    "title": "pushcol",
    "category": "section",
    "text": ""
},

{
    "location": "operations/#[stack](@ref)-and-[unstack](@ref)-1",
    "page": "Table Operations",
    "title": "stack and unstack",
    "category": "section",
    "text": ""
},

{
    "location": "operations/#[summarize](@ref)-1",
    "page": "Table Operations",
    "title": "summarize",
    "category": "section",
    "text": ""
},

{
    "location": "joins/#",
    "page": "Joins",
    "title": "Joins",
    "category": "page",
    "text": "using JuliaDB"
},

{
    "location": "joins/#Joins-1",
    "page": "Joins",
    "title": "Joins",
    "category": "section",
    "text": ""
},

{
    "location": "joins/#Table-Joins-1",
    "page": "Joins",
    "title": "Table Joins",
    "category": "section",
    "text": "Table joins are accomplished through the join function.  "
},

{
    "location": "joins/#Appending-Tables-with-the-Same-Columns-1",
    "page": "Joins",
    "title": "Appending Tables with the Same Columns",
    "category": "section",
    "text": "The merge function will combine tables while maintaining the sorting of the  primary key(s).t1 = table(1:5, rand(5); pkey=1)\nt2 = table(6:10, rand(5); pkey=1)\nmerge(t1, t2)"
},

{
    "location": "onlinestats/#",
    "page": "OnlineStats Integration",
    "title": "OnlineStats Integration",
    "category": "page",
    "text": "using OnlineStats"
},

{
    "location": "onlinestats/#OnlineStats-Integration-1",
    "page": "OnlineStats Integration",
    "title": "OnlineStats Integration",
    "category": "section",
    "text": "OnlineStats is a package for calculating statistics and models with online (one observation at a time) parallelizable algorithms. This integrates tightly with JuliaDB\'s distributed data structures to calculate statistics on large datasets.  The full documentation for OnlineStats is available here."
},

{
    "location": "onlinestats/#Basics-1",
    "page": "OnlineStats Integration",
    "title": "Basics",
    "category": "section",
    "text": "OnlineStats\' objects can be updated with more data and also merged together.  The image below demonstrates what goes on under the hood in JuliaDB to compute a statistic s in parallel.<img src=\"https://user-images.githubusercontent.com/8075494/32748459-519986e8-c88a-11e7-89b3-80dedf7f261b.png\" width=400>OnlineStats integration is available via the reduce and groupreduce functions.  An OnlineStat acts differently from a normal reducer:Normal reducer f:  val = f(val, row)\nOnlineStat reducer o: fit!(o, row)using JuliaDB, OnlineStats\nt = table(1:100, rand(Bool, 100), randn(100));\nreduce(Mean(), t; select = 3)\ngrp = groupreduce(Mean(), t, 2; select=3)\nselect(grp, (1, 2 => value))note: Note\nThe OnlineStats.value function extracts the value of the statistic.  E.g. value(Mean())."
},

{
    "location": "onlinestats/#Calculating-Statistics-on-Multiple-Columns.-1",
    "page": "OnlineStats Integration",
    "title": "Calculating Statistics on Multiple Columns.",
    "category": "section",
    "text": "The OnlineStats.Group type is used for calculating statistics on multiple data streams.  A Group that computes the same OnlineStat can be created through integer multiplication:reduce(3Mean(), t)Alternatively, a Group can be created by providing a collection of OnlineStats.reduce(Group(Extrema(Int), CountMap(Bool), Mean()), t)"
},

{
    "location": "plotting/#",
    "page": "Plotting",
    "title": "Plotting",
    "category": "page",
    "text": ""
},

{
    "location": "plotting/#Plotting-1",
    "page": "Plotting",
    "title": "Plotting",
    "category": "section",
    "text": "using Pkg, Random\nPkg.add(\"StatsPlots\")\nPkg.add(\"GR\")\nusing StatsPlots\nENV[\"GKSwstype\"] = \"100\"\ngr()\nRandom.seed!(1234)  # set random seed to get consistent plots"
},

{
    "location": "plotting/#StatsPlots-1",
    "page": "Plotting",
    "title": "StatsPlots",
    "category": "section",
    "text": "JuliaDB has all access to all the power and flexibility of Plots via StatsPlots and the @df macro.using JuliaDB, StatsPlots\n\nt = table((x = randn(100), y = randn(100)))\n\n@df t scatter(:x, :y)\nsavefig(\"statplot.png\"); nothing # hide(Image: )"
},

{
    "location": "plotting/#Plotting-Big-Data-1",
    "page": "Plotting",
    "title": "Plotting Big Data",
    "category": "section",
    "text": "For large datasets, it isn\'t feasible to render every data point.  The OnlineStats package provides a number of data structures for big data visualization that can be created via the reduce and groupreduce functions.  Example data:using JuliaDB, Plots, OnlineStats\n\nx = randn(10^6)\ny = x + randn(10^6)\nz = x .> 1\nz2 = (x .+ y) .> 0\nt = table((x=x, y=y, z=z, z2=z2))"
},

{
    "location": "plotting/#Mosaic-Plots-1",
    "page": "Plotting",
    "title": "Mosaic Plots",
    "category": "section",
    "text": "A mosaic plot visualizes the bivariate distribution of two categorical variables.  o = reduce(Mosaic(Bool, Bool), t; select = (3, 4))\nplot(o)\npng(\"mosaic.png\"); nothing  # hide(Image: )"
},

{
    "location": "plotting/#Histograms-1",
    "page": "Plotting",
    "title": "Histograms",
    "category": "section",
    "text": "grp = groupreduce(Hist(-5:.5:5), t, :z, select = :x)\nplot(plot.(select(grp, 2))...; link=:all)\npng(\"hist.png\"); nothing # hide(Image: )grp = groupreduce(KHist(20), t, :z, select = :x)\nplot(plot.(select(grp, 2))...; link = :all)\npng(\"hist2.png\"); nothing # hide(Image: )"
},

{
    "location": "plotting/#Partition-and-IndexedPartition-1",
    "page": "Plotting",
    "title": "Partition and IndexedPartition",
    "category": "section",
    "text": "Partition(stat, n) summarizes a univariate data stream.\nThe stat is fitted over n approximately equal-sized pieces.\nIndexedPartition(T, stat, n) summarizes a bivariate data stream.\nThe stat is fitted over n pieces covering the domain of another variable of type T.o = reduce(Partition(KHist(10), 50), t; select=:y)\nplot(o)\npng(\"partition.png\"); nothing # hide(Image: )o = reduce(IndexedPartition(Float64, KHist(10), 50), t; select=(:x, :y))\nplot(o)\npng(\"partition2.png\"); nothing # hide(Image: )"
},

{
    "location": "plotting/#GroupBy-1",
    "page": "Plotting",
    "title": "GroupBy",
    "category": "section",
    "text": "o = reduce(GroupBy{Bool}(KHist(20)), t; select = (:z, :x))\nplot(o)\npng(\"groupby.png\"); nothing # hide(Image: )"
},

{
    "location": "plotting/#Convenience-function-for-Partition-and-IndexedPartition-1",
    "page": "Plotting",
    "title": "Convenience function for Partition and IndexedPartition",
    "category": "section",
    "text": "You can also use the partitionplot function, a slightly less verbose way of plotting Partition and IndexedPartition objects.# x by itself\npartitionplot(t, :x, stat = Extrema())\nsavefig(\"partitionplot1.png\"); nothing # hide(Image: )# y by x, grouped by z\npartitionplot(t, :x, :y, stat = Extrema(), by = :z)\nsavefig(\"partitionplot2.png\"); nothing # hide(Image: )"
},

{
    "location": "missing_values/#",
    "page": "Missing Values",
    "title": "Missing Values",
    "category": "page",
    "text": "using JuliaDB"
},

{
    "location": "missing_values/#Missing-Values-1",
    "page": "Missing Values",
    "title": "Missing Values",
    "category": "section",
    "text": "Julia has several different ways of representing missing data.  If a column of data may contain missing values, JuliaDB supports both missing value representations of Union{T, Missing} and DataValue{T}.While Union{T, Missing} is the default representation, functions that generate missing values (join) have a missingtype = Missing keyword argument that can be set to DataValue.The convertmissing function is used to switch the representation of missing values.using DataValues\nconvertmissing(table([1, NA]), Missing)\nconvertmissing(table([1, missing]), DataValue)The dropmissing function will remove rows that contain Missing or missing DataValues.dropmissing(table([1, NA]))\ndropmissing(table([1, missing]))"
},

{
    "location": "out_of_core/#",
    "page": "Out-of-core processing",
    "title": "Out-of-core processing",
    "category": "page",
    "text": ""
},

{
    "location": "out_of_core/#Out-of-core-processing-1",
    "page": "Out-of-core processing",
    "title": "Out-of-core processing",
    "category": "section",
    "text": "JuliaDB can load data that is too big to fit in memory (RAM) as well as run a subset of operations on big tables.  In particular, OnlineStats Integration works with reduce and groupreduce for running statistical analyses that traditionally would not be possible!"
},

{
    "location": "out_of_core/#Processing-Scheme-1",
    "page": "Out-of-core processing",
    "title": "Processing Scheme",
    "category": "section",
    "text": "Data is loaded into a distributed dataset containing \"chunks\" that safely fit in memory. \nData is processed Distributed.nworkers() chunks at a time (each worker processes a chunk and then moves onto the next chunk).\nNote: This means Distributed.nworkers() * avg_size_of_chunk will be in RAM simultaneously.\nOutput data is accumulated in-memory.The limitations of this processing scheme is that only certain operations work out-of-core:loadtable\nloadndsparse\nload\nreduce\ngroupreduce\njoin (see Join to Big Table)"
},

{
    "location": "out_of_core/#Loading-Data-1",
    "page": "Out-of-core processing",
    "title": "Loading Data",
    "category": "section",
    "text": "The loadtable and loadndsparse functions accept the keyword arguments output and chunks that specify the directory to save the data into and the number of chunks to be generated from the input files, respectively.Here\'s an example:loadtable(glob(\"*.csv\"), output=\"bin\", chunks=100; kwargs...)Suppose there are 800 .csv files in the current directory.  They will be read into 100 chunks (8 files per chunk).  Each worker process will load 8 files into memory, save the chunk into a single binary file in the bin directory, and move onto the next 8 files.note: Note\nDistributed.nworkers() * (number_of_csvs / chunks) needs to fit in memory simultaneously.Once data has been loaded in this way, you can reload the dataset (extremely fast) viatbl = load(\"bin\")"
},

{
    "location": "out_of_core/#[reduce](@ref)-and-[groupreduce](@ref)-Operations-1",
    "page": "Out-of-core processing",
    "title": "reduce and groupreduce Operations",
    "category": "section",
    "text": "reduce is the simplest out-of-core operation since it works pair-wise.  You can also perform group-by operations with a reducer via groupreduce.using JuliaDB, OnlineStats\n\nx = rand(Bool, 100)\ny = x + randn(100)\n\nt = table((x=x, y=y))\n\ngroupreduce(+, t, :x; select = :y)You can replace the reducer with any OnlineStat object (see OnlineStats Integration for more details):groupreduce(Sum(), t, :x; select = :y)"
},

{
    "location": "out_of_core/#Join-to-Big-Table-1",
    "page": "Out-of-core processing",
    "title": "Join to Big Table",
    "category": "section",
    "text": "join operations have limited out-of-core support. Specifically,join(bigtable, smalltable; broadcast=:right, how=:inner|:left|:anti)Here bigtable can be larger than memory, while Distributed.nworkers() copies of smalltable must fit in memory. Note that only :inner, :left, and :anti joins are supported (no :outer joins). In this operation, smalltable is first broadcast to all processors and bigtable is joined Distributed.nworkers() chunks at a time."
},

{
    "location": "ml/#",
    "page": "Feature Extraction",
    "title": "Feature Extraction",
    "category": "page",
    "text": ""
},

{
    "location": "ml/#Feature-Extraction-1",
    "page": "Feature Extraction",
    "title": "Feature Extraction",
    "category": "section",
    "text": "Machine learning models are composed of mathematical operations on matrices of numbers. However, data in the real world is often in tabular form containing more than just numbers. Hence, the first step in applying machine learning is to turn such tabular non-numeric data into a matrix of numbers. Such matrices are called \"feature matrices\". JuliaDB contains an ML module which has helper functions to extract feature matrices.In this document, we will turn the titanic dataset from Kaggle into numeric form and apply a machine learning model on it.using JuliaDB\n\ndownload(\"https://raw.githubusercontent.com/agconti/\"*\n          \"kaggle-titanic/master/data/train.csv\", \"train.csv\")\n\ntrain_table = loadtable(\"train.csv\", escapechar=\'\"\')\npopcol(popcol(popcol(train_table, :Name), :Ticket), :Cabin) # hide"
},

{
    "location": "ml/#ML.schema-1",
    "page": "Feature Extraction",
    "title": "ML.schema",
    "category": "section",
    "text": "Schema is a programmatic description of the data in each column. It is a dictionary which maps each column (by name) to its schema type (mainly Continuous, and Categorical).ML.Continuous: data is drawn from the real number line (e.g. Age)\nML.Categorical: data is drawn from a fixed set of values (e.g. Sex)ML.schema(train_table) will go through the data and infer the types and distribution of data. Let\'s try it without any arguments on the titanic dataset:using JuliaDB: ML\n\nML.schema(train_table)Here is how the schema was inferred:Numeric fields were inferred to be Continuous, their mean and standard deviations were computed. This will later be used in normalizing the column in the feature matrix using the formula ((value - mean) / standard_deviation). This will bring all columns to the same \"scale\" making the training more effective.\nSome string columns are inferred to be Categorical (e.g. Sex, Embarked) - this means that the column is a PooledArray, and is drawn from a small \"pool\" of values. For example Sex is either \"male\" or \"female\"; Embarked is one of \"Q\", \"S\", \"C\" or \"\"\nSome string columns (e.g. Name) get the schema nothing – such columns usually contain unique identifying data, so are not useful in machine learning.\nThe age column was inferred as Maybe{Continuous} – this means that there are missing values in the column. The mean and standard deviation computed are for the non-missing values.You may note that Survived column contains only 1s and 0s to denote whether a passenger survived the disaster or not. However, our schema inferred the column to be Continuous. To not be overly presumptive ML.schema will assume all numeric columns are continuous by default. We can give the hint that the Survived column is categorical by passing the hints arguemnt as a dictionary of column name to schema type. Further, we will also treat Pclass (passenger class) as categorical and suppress Parch and SibSp fields.sch = ML.schema(train_table, hints=Dict(\n        :Pclass => ML.Categorical,\n        :Survived => ML.Categorical,\n        :Parch => nothing,\n        :SibSp => nothing,\n        :Fare => nothing,\n        )\n)"
},

{
    "location": "ml/#Split-schema-into-input-and-output-1",
    "page": "Feature Extraction",
    "title": "Split schema into input and output",
    "category": "section",
    "text": "In a machine learning model, a subset of fields act as the input to the model, and one or more fields act as the output (predicted variables). For example, in the titanic dataset, you may want to predict whether a person will survive or not. So \"Survived\" field will be the output column. Using the ML.splitschema function, you can split the schema into input and output schema.input_sch, output_sch = ML.splitschema(sch, :Survived)"
},

{
    "location": "ml/#Extracting-feature-matrix-1",
    "page": "Feature Extraction",
    "title": "Extracting feature matrix",
    "category": "section",
    "text": "Once the schema has been created, you can extract the feature matrix according to the given schema using ML.featuremat:train_input = ML.featuremat(input_sch, train_table)train_output = ML.featuremat(output_sch, train_table)"
},

{
    "location": "ml/#Learning-1",
    "page": "Feature Extraction",
    "title": "Learning",
    "category": "section",
    "text": "Let us create a simple neural network to learn whether a passenger will survive or not using the Flux framework.ML.width(schema) will give the number of features in the schema we will use this in specifying the model size:using Flux\n\nmodel = Chain(\n  Dense(ML.width(input_sch), 32, relu),\n  Dense(32, ML.width(output_sch)),\n  softmax)\n\nloss(x, y) = Flux.mse(model(x), y)\nopt = Flux.ADAM(Flux.params(model))\nevalcb = Flux.throttle(() -> @show(loss(first(data)...)), 2);Train the data in 10 iterationsdata = [(train_input, train_output)]\nfor i = 1:10\n  Flux.train!(loss, data, opt, cb = evalcb)\nenddata given to the model is a vector of batches of input-output matrices. In this case we are training with just 1 batch."
},

{
    "location": "ml/#Prediction-1",
    "page": "Feature Extraction",
    "title": "Prediction",
    "category": "section",
    "text": "Now let\'s load some testing data to use the model we learned to predict survival.\ndownload(\"https://raw.githubusercontent.com/agconti/\"*\n          \"kaggle-titanic/master/data/test.csv\", \"test.csv\")\n\ntest_table = loadtable(\"test.csv\", escapechar=\'\"\')\n\ntest_input = ML.featuremat(input_sch, test_table) ;Run the model on one observation:model(test_input[:, 1])The output has two numbers which add up to 1: the probability of not surviving vs that of surviving. It seems, according to our model, that this person is unlikely to survive on the titanic.You can also run the model on all observations by simply passing the whole feature matrix to model.model(test_input)"
},

{
    "location": "tutorial/#",
    "page": "Tutorial",
    "title": "Tutorial",
    "category": "page",
    "text": ""
},

{
    "location": "tutorial/#Tutorial-1",
    "page": "Tutorial",
    "title": "Tutorial",
    "category": "section",
    "text": ""
},

{
    "location": "tutorial/#Introduction-1",
    "page": "Tutorial",
    "title": "Introduction",
    "category": "section",
    "text": "This is a port of a well known tutorial for the JuliaDB package. This tutorial is available as a Jupyter notebook here."
},

{
    "location": "tutorial/#Getting-the-data-1",
    "page": "Tutorial",
    "title": "Getting the data",
    "category": "section",
    "text": "The flights dataset for the tutorial is here.  Alternatively, run the following in Julia:download(\"https://raw.githubusercontent.com/piever/JuliaDBTutorial/master/hflights.csv\")"
},

{
    "location": "tutorial/#Loading-the-data-1",
    "page": "Tutorial",
    "title": "Loading the data",
    "category": "section",
    "text": "Loading a csv file is straightforward with JuliaDB:using JuliaDB\n\nflights = loadtable(\"hflights.csv\")Of course, replace the path with the location of the dataset you have just downloaded."
},

{
    "location": "tutorial/#Filtering-the-data-1",
    "page": "Tutorial",
    "title": "Filtering the data",
    "category": "section",
    "text": "In order to select only rows matching certain criteria, use the filter function:filter(i -> (i.Month == 1) && (i.DayofMonth == 1), flights)To test if one of two conditions is verified:filter(i -> (i.UniqueCarrier == \"AA\") || (i.UniqueCarrier == \"UA\"), flights)\n\n# in this case, you can simply test whether the `UniqueCarrier` is in a given list:\n\nfilter(i -> i.UniqueCarrier in [\"AA\", \"UA\"], flights)"
},

{
    "location": "tutorial/#Select:-pick-columns-by-name-1",
    "page": "Tutorial",
    "title": "Select: pick columns by name",
    "category": "section",
    "text": "You can use the select function to select a subset of columns:select(flights, (:DepTime, :ArrTime, :FlightNum))Table with 227496 rows, 3 columns:\nDepTime  ArrTime  FlightNum\n───────────────────────────\n1400     1500     428\n1401     1501     428\n1352     1502     428\n1403     1513     428\n1405     1507     428\n1359     1503     428\n1359     1509     428\n1355     1454     428\n1443     1554     428\n1443     1553     428\n1429     1539     428\n1419     1515     428\n⋮\n1939     2119     124\n556      745      280\n1026     1208     782\n1611     1746     1050\n758      1051     201\n1307     1600     471\n1818     2111     1191\n2047     2334     1674\n912      1031     127\n656      812      621\n1600     1713     1597Let\'s select all columns between :Year and :Month as well as all columns containing \"Taxi\" or \"Delay\" in their name. Between selects columns between two specified extremes, passing a function filters column names by that function and All takes the union of all selectors (or all columns, if no selector is specified).select(flights, All(Between(:Year, :DayofMonth), i -> occursin(\"Taxi\", string(i)), i -> occursin(\"Delay\", string(i))))Table with 227496 rows, 7 columns:\nYear  Month  DayofMonth  TaxiIn  TaxiOut  ArrDelay  DepDelay\n────────────────────────────────────────────────────────────\n2011  1      1           7       13       -10       0\n2011  1      2           6       9        -9        1\n2011  1      3           5       17       -8        -8\n2011  1      4           9       22       3         3\n2011  1      5           9       9        -3        5\n2011  1      6           6       13       -7        -1\n2011  1      7           12      15       -1        -1\n2011  1      8           7       12       -16       -5\n2011  1      9           8       22       44        43\n2011  1      10          6       19       43        43\n2011  1      11          8       20       29        29\n2011  1      12          4       11       5         19\n⋮\n2011  12     6           4       15       14        39\n2011  12     6           13      9        -10       -4\n2011  12     6           4       12       -12       1\n2011  12     6           3       9        -9        16\n2011  12     6           3       10       -4        -2\n2011  12     6           5       10       0         7\n2011  12     6           5       11       -9        8\n2011  12     6           4       9        4         7\n2011  12     6           4       14       -4        -3\n2011  12     6           3       9        -13       -4\n2011  12     6           3       11       -12       0The same could be achieved more concisely using regular expressions:select(flights, All(Between(:Year, :DayofMonth), r\"Taxi|Delay\"))"
},

{
    "location": "tutorial/#Applying-several-operations-1",
    "page": "Tutorial",
    "title": "Applying several operations",
    "category": "section",
    "text": "If one wants to apply several operations one after the other, there are two main approaches:nesting\npipingLet\'s assume we want to select UniqueCarrier and DepDelay columns and filter for delays over 60 minutes. Since the DepDelay column has missing data, we also need to filter out missing values via !ismissing.  The nesting approach would be:filter(i -> !ismissing(i.DepDelay > 60), select(flights, (:UniqueCarrier, :DepDelay)))Table with 224591 rows, 2 columns:\nUniqueCarrier  DepDelay\n───────────────────────\n\"AA\"           0\n\"AA\"           1\n\"AA\"           -8\n\"AA\"           3\n\"AA\"           5\n\"AA\"           -1\n\"AA\"           -1\n\"AA\"           -5\n\"AA\"           43\n\"AA\"           43\n⋮\n\"WN\"           1\n\"WN\"           16\n\"WN\"           -2\n\"WN\"           7\n\"WN\"           8\n\"WN\"           7\n\"WN\"           -3\n\"WN\"           -4\n\"WN\"           0For piping, we\'ll use the excellent Lazy package.import Lazy\nLazy.@as x flights begin\n    select(x, (:UniqueCarrier, :DepDelay))\n    filter(i -> !ismissing(i.DepDelay > 60), x)\nendTable with 224591 rows, 2 columns:\nUniqueCarrier  DepDelay\n───────────────────────\n\"AA\"           0\n\"AA\"           1\n\"AA\"           -8\n\"AA\"           3\n\"AA\"           5\n\"AA\"           -1\n\"AA\"           -1\n\"AA\"           -5\n\"AA\"           43\n\"AA\"           43\n⋮\n\"WN\"           1\n\"WN\"           16\n\"WN\"           -2\n\"WN\"           7\n\"WN\"           8\n\"WN\"           7\n\"WN\"           -3\n\"WN\"           -4\n\"WN\"           0where the variable x denotes our data at each stage. At the beginning it is flights, then it only has the two relevant columns and, at the last step, it is filtered."
},

{
    "location": "tutorial/#Reorder-rows-1",
    "page": "Tutorial",
    "title": "Reorder rows",
    "category": "section",
    "text": "Select UniqueCarrier and DepDelay columns and sort by DepDelay:sort(flights, :DepDelay, select = (:UniqueCarrier, :DepDelay))Table with 227496 rows, 2 columns:\nUniqueCarrier  DepDelay\n───────────────────────\n\"OO\"           -33\n\"MQ\"           -23\n\"XE\"           -19\n\"XE\"           -19\n\"CO\"           -18\n\"EV\"           -18\n\"XE\"           -17\n\"CO\"           -17\n\"XE\"           -17\n\"MQ\"           -17\n\"XE\"           -17\n\"DL\"           -17\n⋮\n\"US\"           missing\n\"US\"           missing\n\"US\"           missing\n\"WN\"           missing\n\"WN\"           missing\n\"WN\"           missing\n\"WN\"           missing\n\"WN\"           missing\n\"WN\"           missing\n\"WN\"           missing\n\"WN\"           missingor, in reverse order:sort(flights, :DepDelay, select = (:UniqueCarrier, :DepDelay), rev = true)"
},

{
    "location": "tutorial/#Apply-a-function-row-by-row-1",
    "page": "Tutorial",
    "title": "Apply a function row by row",
    "category": "section",
    "text": "To apply a function row by row, use map: the first argument is the anonymous function, the second is the dataset.speed = map(i -> i.Distance / i.AirTime * 60, flights)227496-element Array{Union{Missing, Float64},1}:\n 336.0\n 298.6666666666667\n 280.0\n 344.61538461538464\n 305.45454545454544\n 298.6666666666667\n 312.55813953488376\n 336.0\n 327.8048780487805\n 298.6666666666667\n 320.0\n ⋮\n 473.7931034482758\n 479.30232558139534\n 496.6265060240964\n 468.59999999999997\n 478.1632653061224\n 483.0927835051546\n 498.5106382978723\n 445.57377049180326\n 424.6875\n 460.6779661016949"
},

{
    "location": "tutorial/#Add-new-variables-1",
    "page": "Tutorial",
    "title": "Add new variables",
    "category": "section",
    "text": "Use the pushcol function to add a column to an existing dataset:pushcol(flights, :Speed, speed)If you need to add the new column to the existing dataset:flights = pushcol(flights, :Speed, speed)"
},

{
    "location": "tutorial/#Reduce-variables-to-values-1",
    "page": "Tutorial",
    "title": "Reduce variables to values",
    "category": "section",
    "text": "To get the average delay, we first filter away datapoints where ArrDelay is missing, then group by :Dest, select :ArrDelay and compute the mean:using Statistics\n\ngroupby(mean ∘ skipmissing, flights, :Dest, select = :ArrDelay)Table with 116 rows, 2 columns:\nDest   avg_delay\n────────────────\n\"ABQ\"  7.22626\n\"AEX\"  5.83944\n\"AGS\"  4.0\n\"AMA\"  6.8401\n\"ANC\"  26.0806\n\"ASE\"  6.79464\n\"ATL\"  8.23325\n\"AUS\"  7.44872\n\"AVL\"  9.97399\n\"BFL\"  -13.1988\n\"BHM\"  8.69583\n\"BKG\"  -16.2336\n⋮\n\"SJU\"  11.5464\n\"SLC\"  1.10485\n\"SMF\"  4.66271\n\"SNA\"  0.35801\n\"STL\"  7.45488\n\"TPA\"  4.88038\n\"TUL\"  6.35171\n\"TUS\"  7.80168\n\"TYS\"  11.3659\n\"VPS\"  12.4572\n\"XNA\"  6.89628"
},

{
    "location": "tutorial/#Performance-tip-1",
    "page": "Tutorial",
    "title": "Performance tip",
    "category": "section",
    "text": "If you\'ll group often by the same variable, you can sort your data by that variable at once to optimize future computations.sortedflights = reindex(flights, :Dest)Table with 227496 rows, 22 columns:\nColumns:\n#   colname            type\n────────────────────────────────────────────────────\n1   Dest               String\n2   Year               Int64\n3   Month              Int64\n4   DayofMonth         Int64\n5   DayOfWeek          Int64\n6   DepTime            DataValues.DataValue{Int64}\n7   ArrTime            DataValues.DataValue{Int64}\n8   UniqueCarrier      String\n9   FlightNum          Int64\n10  TailNum            String\n11  ActualElapsedTime  DataValues.DataValue{Int64}\n12  AirTime            DataValues.DataValue{Int64}\n13  ArrDelay           DataValues.DataValue{Int64}\n14  DepDelay           DataValues.DataValue{Int64}\n15  Origin             String\n16  Distance           Int64\n17  TaxiIn             DataValues.DataValue{Int64}\n18  TaxiOut            DataValues.DataValue{Int64}\n19  Cancelled          Int64\n20  CancellationCode   String\n21  Diverted           Int64\n22  Speed              DataValues.DataValue{Float64}using BenchmarkTools\n\nprintln(\"Presorted timing:\")\n@benchmark groupby(mean ∘ skipmissing, sortedflights, select = :ArrDelay)Presorted timing:\n\nBenchmarkTools.Trial:\n  memory estimate:  31.23 MiB\n  allocs estimate:  1588558\n  --------------\n  minimum time:     39.565 ms (8.03% GC)\n  median time:      44.401 ms (9.83% GC)\n  mean time:        44.990 ms (10.36% GC)\n  maximum time:     57.016 ms (15.96% GC)\n  --------------\n  samples:          112\n  evals/sample:     1println(\"Non presorted timing:\")\n@benchmark groupby(mean ∘ skipmissing, flights, select = :ArrDelay)Non presorted timing:\n\nBenchmarkTools.Trial:\n  memory estimate:  1.81 KiB\n  allocs estimate:  30\n  --------------\n  minimum time:     195.095 μs (0.00% GC)\n  median time:      212.309 μs (0.00% GC)\n  mean time:        230.878 μs (0.20% GC)\n  maximum time:     4.859 ms (95.04% GC)\n  --------------\n  samples:          10000\n  evals/sample:     1Using summarize, we can summarize several columns at the same time:summarize(mean ∘ skipmissing, flights, :Dest, select = (:Cancelled, :Diverted))\n\n# For each carrier, calculate the minimum and maximum arrival and departure delays:\n\ncols = Tuple(findall(i -> occursin(\"Delay\", string(i)), colnames(flights)))\nsummarize((min = minimum∘skipmissing, max = maximum∘skipmissing), flights, :UniqueCarrier, select = cols)Table with 15 rows, 5 columns:\nUniqueCarrier  ArrDelay_min  DepDelay_min  ArrDelay_max  DepDelay_max\n─────────────────────────────────────────────────────────────────────\n\"AA\"           -39           -15           978           970\n\"AS\"           -43           -15           183           172\n\"B6\"           -44           -14           335           310\n\"CO\"           -55           -18           957           981\n\"DL\"           -32           -17           701           730\n\"EV\"           -40           -18           469           479\n\"F9\"           -24           -15           277           275\n\"FL\"           -30           -14           500           507\n\"MQ\"           -38           -23           918           931\n\"OO\"           -57           -33           380           360\n\"UA\"           -47           -11           861           869\n\"US\"           -42           -17           433           425\n\"WN\"           -44           -10           499           548\n\"XE\"           -70           -19           634           628\n\"YV\"           -32           -11           72            54For each day of the year, count the total number of flights and sort in descending order:Lazy.@as x flights begin\n    groupby(length, x, :DayofMonth)\n    sort(x, :length, rev = true)\nendTable with 31 rows, 2 columns:\nDayofMonth  length\n──────────────────\n28          7777\n27          7717\n21          7698\n14          7694\n7           7621\n18          7613\n6           7606\n20          7599\n11          7578\n13          7546\n10          7541\n17          7537\n⋮\n25          7406\n16          7389\n8           7366\n12          7301\n4           7297\n19          7295\n24          7234\n5           7223\n30          6728\n29          6697\n31          4339For each destination, count the total number of flights and the number of distinct planes that flew theregroupby((flight_count = length, plane_count = length∘union), flights, :Dest, select = :TailNum)Table with 116 rows, 3 columns:\nDest   flight_count  plane_count\n────────────────────────────────\n\"ABQ\"  2812          716\n\"AEX\"  724           215\n\"AGS\"  1             1\n\"AMA\"  1297          158\n\"ANC\"  125           38\n\"ASE\"  125           60\n\"ATL\"  7886          983\n\"AUS\"  5022          1015\n\"AVL\"  350           142\n\"BFL\"  504           70\n\"BHM\"  2736          616\n\"BKG\"  110           63\n⋮\n\"SJU\"  391           115\n\"SLC\"  2033          368\n\"SMF\"  1014          184\n\"SNA\"  1661          67\n\"STL\"  2509          788\n\"TPA\"  3085          697\n\"TUL\"  2924          771\n\"TUS\"  1565          226\n\"TYS\"  1210          227\n\"VPS\"  880           224\n\"XNA\"  1172          177"
},

{
    "location": "tutorial/#Window-functions-1",
    "page": "Tutorial",
    "title": "Window functions",
    "category": "section",
    "text": "In the previous section, we always applied functions that reduced a table or vector to a single value. Window functions instead take a vector and return a vector of the same length, and can also be used to manipulate data. For example we can rank, within each UniqueCarrier, how much delay a given flight had and figure out the day and month with the two greatest delays:using StatsBase\nfc = dropmissing(flights, :DepDelay)\ngfc = groupby(fc, :UniqueCarrier, select = (:Month, :DayofMonth, :DepDelay), flatten = true) do dd\n    rks = ordinalrank(column(dd, :DepDelay), rev = true)\n    sort(dd[rks .<= 2], by =  i -> i.DepDelay, rev = true)\nendTable with 30 rows, 4 columns:\nUniqueCarrier  Month  DayofMonth  DepDelay\n──────────────────────────────────────────\n\"AA\"           12     12          970\n\"AA\"           11     19          677\n\"AS\"           2      28          172\n\"AS\"           7      6           138\n\"B6\"           10     29          310\n\"B6\"           8      19          283\n\"CO\"           8      1           981\n\"CO\"           1      20          780\n\"DL\"           10     25          730\n\"DL\"           4      5           497\n\"EV\"           6      25          479\n\"EV\"           1      5           465\n⋮\n\"OO\"           4      4           343\n\"UA\"           6      21          869\n\"UA\"           9      18          588\n\"US\"           4      19          425\n\"US\"           8      26          277\n\"WN\"           4      8           548\n\"WN\"           9      29          503\n\"XE\"           12     29          628\n\"XE\"           12     29          511\n\"YV\"           4      22          54\n\"YV\"           4      30          46Though in this case, it would have been simpler to use Julia partial sorting:groupby(fc, :UniqueCarrier, select = (:Month, :DayofMonth, :DepDelay), flatten = true) do dd\n    partialsort(dd, 1:2, by = i -> i.DepDelay, rev = true)\nendTable with 30 rows, 4 columns:\nUniqueCarrier  Month  DayofMonth  DepDelay\n──────────────────────────────────────────\n\"AA\"           12     12          970\n\"AA\"           11     19          677\n\"AS\"           2      28          172\n\"AS\"           7      6           138\n\"B6\"           10     29          310\n\"B6\"           8      19          283\n\"CO\"           8      1           981\n\"CO\"           1      20          780\n\"DL\"           10     25          730\n\"DL\"           4      5           497\n\"EV\"           6      25          479\n\"EV\"           1      5           465\n⋮\n\"OO\"           4      4           343\n\"UA\"           6      21          869\n\"UA\"           9      18          588\n\"US\"           4      19          425\n\"US\"           8      26          277\n\"WN\"           4      8           548\n\"WN\"           9      29          503\n\"XE\"           12     29          628\n\"XE\"           12     29          511\n\"YV\"           4      22          54\n\"YV\"           4      30          46For each month, calculate the number of flights and the change from the previous monthusing ShiftedArrays\ny = groupby(length, flights, :Month)\nlengths = columns(y, :length)\npushcol(y, :change, lengths .- lag(lengths))Table with 12 rows, 3 columns:\nMonth  length  change\n─────────────────────\n1      18910   missing\n2      17128   -1782\n3      19470   2342\n4      18593   -877\n5      19172   579\n6      19600   428\n7      20548   948\n8      20176   -372\n9      18065   -2111\n10     18696   631\n11     18021   -675\n12     19117   1096"
},

{
    "location": "tutorial/#Visualizing-your-data-1",
    "page": "Tutorial",
    "title": "Visualizing your data",
    "category": "section",
    "text": "The StatsPlots and GroupedErrors package as well as native plotting recipes from JuliaDB using OnlineStats make a rich set of visualizations possible with an intuitive syntax.Use the @df macro to be able to refer to columns simply by their name. You can work with these symobls as if they are regular vectors. Here for example, we split data according to whether the distance is smaller or bigger than 1000.using StatsPlots\ngr(fmt = :png) # choose the fast GR backend and set format to png: svg would probably crash with so many points\n@df flights scatter(:DepDelay, :ArrDelay, group = :Distance .> 1000, layout = 2, legend = :topleft)(Image: scatterflights)"
},

{
    "location": "tutorial/#Online-statistics-1",
    "page": "Tutorial",
    "title": "Online statistics",
    "category": "section",
    "text": "For large datasets, summary statistics can be computed using efficient online algorithms implemnted in OnlineStats. Here we will use an online algorithm to compute the mean traveled distance split across month of the year.using OnlineStats\ngrpred = groupreduce(Mean(), flights, :Month; select = :Distance)Table with 12 rows, 2 columns:\nMonth  Mean\n────────────────────────────────────\n1      Mean: n=18910 | value=760.804\n2      Mean: n=17128 | value=763.909\n3      Mean: n=19470 | value=782.788\n4      Mean: n=18593 | value=783.845\n5      Mean: n=19172 | value=789.66\n6      Mean: n=19600 | value=797.869\n7      Mean: n=20548 | value=798.52\n8      Mean: n=20176 | value=793.727\n9      Mean: n=18065 | value=790.444\n10     Mean: n=18696 | value=788.256\n11     Mean: n=18021 | value=790.691\n12     Mean: n=19117 | value=809.024Extract the values of the OnlineStat objects with the value function.select(grpred, (:Month, :Mean => value))Table with 12 rows, 2 columns:\nMonth  Mean\n──────────────\n1      760.804\n2      763.909\n3      782.788\n4      783.845\n5      789.66\n6      797.869\n7      798.52\n8      793.727\n9      790.444\n10     788.256\n11     790.691\n12     809.024"
},

{
    "location": "tutorial/#Interfacing-with-online-datasets-1",
    "page": "Tutorial",
    "title": "Interfacing with online datasets",
    "category": "section",
    "text": "JuliaDB can also smoothly interface online datasets using packages from the JuliaDatabases organization. Here\'s how it would work with a MySQL dataset:using MySQL, JuliaDBconn = MySQL.connect(host::String, user::String, passwd::String; db::String = \"\") # edit as needed for your dataset\nMySQL.query(conn, \"SELECT Name, Salary FROM Employee;\") |> table # execute the query and collect as a table\nMySQL.disconnect(conn)"
},

{
    "location": "api/#",
    "page": "API",
    "title": "API",
    "category": "page",
    "text": ""
},

{
    "location": "api/#Dagger.compute-Tuple{JuliaDB.DNDSparse}",
    "page": "API",
    "title": "Dagger.compute",
    "category": "method",
    "text": "compute(t::DNDSparse; allowoverlap, closed)\n\nComputes any delayed-evaluations in the DNDSparse. The computed data is left on the worker processes. Subsequent operations on the results will reuse the chunks.\n\nIf allowoverlap is false then the computed data is re-sorted if required to have no chunks with overlapping index ranges if necessary.\n\nIf closed is true then the computed data is re-sorted if required to have no chunks with overlapping OR continuous boundaries.\n\nSee also collect.\n\nwarning: Warning\ncompute(t) requires at least as much memory as the size of the result of the computing t. You usually don\'t need to do this for the whole dataset. If the result is expected to be big, try compute(save(t, \"output_dir\")) instead. See save for more.\n\n\n\n\n\n"
},

{
    "location": "api/#Dagger.distribute",
    "page": "API",
    "title": "Dagger.distribute",
    "category": "function",
    "text": "distribute(itable::NDSparse, nchunks::Int=nworkers())\n\nDistributes an NDSparse object into a DNDSparse of nchunks chunks of approximately equal size.\n\nReturns a DNDSparse.\n\n\n\n\n\n"
},

{
    "location": "api/#Dagger.distribute-Tuple{IndexedTable,Any}",
    "page": "API",
    "title": "Dagger.distribute",
    "category": "method",
    "text": "distribute(t::Table, chunks)\n\nDistribute a table in chunks pieces. Equivalent to table(t, chunks=chunks).\n\n\n\n\n\n"
},

{
    "location": "api/#Dagger.distribute-Union{Tuple{V}, Tuple{NDSparse{V,D,C,V1} where V1<:(AbstractArray{T,1} where T) where C<:(StructArray{T,1,C} where C<:NamedTuple where T) where D<:Tuple,AbstractArray}} where V",
    "page": "API",
    "title": "Dagger.distribute",
    "category": "method",
    "text": "distribute(itable::NDSparse, rowgroups::AbstractArray)\n\nDistributes an NDSparse object into a DNDSparse by splitting it up into chunks of rowgroups elements. rowgroups is a vector specifying the number of rows in the chunks.\n\nReturns a DNDSparse.\n\n\n\n\n\n"
},

{
    "location": "api/#Dagger.load-Tuple{AbstractString}",
    "page": "API",
    "title": "Dagger.load",
    "category": "method",
    "text": "load(dir::AbstractString)\n\nLoad a saved DNDSparse from dir directory. Data can be saved using the save function.\n\n\n\n\n\n"
},

{
    "location": "api/#Dagger.save-Tuple{Union{DIndexedTable, DNDSparse},AbstractString}",
    "page": "API",
    "title": "Dagger.save",
    "category": "method",
    "text": "save(t::Union{DNDSparse, DIndexedTable}, destdir::AbstractString)\n\nSaves a distributed dataset to disk in directory destdir. Saved data can be loaded with load.\n\n\n\n\n\n"
},

{
    "location": "api/#Dagger.save-Tuple{Union{IndexedTable, NDSparse},AbstractString}",
    "page": "API",
    "title": "Dagger.save",
    "category": "method",
    "text": "save(t::Union{NDSparse, IndexedTable}, dest::AbstractString)\n\nSave a dataset to disk as dest.  Saved data can be loaded with load.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.convertdim-Union{Tuple{V}, Tuple{K}, Tuple{DNDSparse{K,V},Union{Int64, Symbol},Any}} where V where K",
    "page": "API",
    "title": "IndexedTables.convertdim",
    "category": "method",
    "text": "convertdim(x::DNDSparse, d::DimName, xlate; agg::Function, name)\n\nApply function or dictionary xlate to each index in the specified dimension. If the mapping is many-to-one, agg is used to aggregate the results. name optionally specifies a name for the new dimension. xlate must be a monotonically increasing function.\n\nSee also reduce\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.leftjoin-Union{Tuple{V}, Tuple{K}, Tuple{Any,DNDSparse{K,V},DNDSparse}, Tuple{Any,DNDSparse{K,V},DNDSparse,Any}, Tuple{Any,DNDSparse{K,V},DNDSparse,Any,Any}} where V where K",
    "page": "API",
    "title": "IndexedTables.leftjoin",
    "category": "method",
    "text": "leftjoin(left::DNDSparse, right::DNDSparse, [op::Function])\n\nKeeps only rows with indices in left. If rows of the same index are present in right, then they are combined using op. op by default picks the value from right.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.naturaljoin-Union{Tuple{D2}, Tuple{D1}, Tuple{I2}, Tuple{I1}, Tuple{Any,DNDSparse{I1,D1},DNDSparse{I2,D2}}} where D2 where D1 where I2 where I1",
    "page": "API",
    "title": "IndexedTables.naturaljoin",
    "category": "method",
    "text": "naturaljoin(op, left::DNDSparse, right::DNDSparse, ascolumns=false)\n\nReturns a new DNDSparse containing only rows where the indices are present both in left AND right tables. The data columns are concatenated. The data of the matching rows from left and right are combined using op. If op returns a tuple or NamedTuple, and ascolumns is set to true, the output table will contain the tuple elements as separate data columns instead as a single column of resultant tuples.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.naturaljoin-Union{Tuple{D2}, Tuple{D1}, Tuple{J}, Tuple{I}, Tuple{DNDSparse{I,D1},DNDSparse{J,D2}}} where D2 where D1 where J where I",
    "page": "API",
    "title": "IndexedTables.naturaljoin",
    "category": "method",
    "text": "naturaljoin(left::DNDSparse, right::DNDSparse, [op])\n\nReturns a new DNDSparse containing only rows where the indices are present both in left AND right tables. The data columns are concatenated.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.reducedim_vec-Tuple{Any,JuliaDB.DNDSparse,Any}",
    "page": "API",
    "title": "IndexedTables.reducedim_vec",
    "category": "method",
    "text": "reducedim_vec(f::Function, t::DNDSparse, dims)\n\nLike reducedim, except uses a function mapping a vector of values to a scalar instead of a 2-argument scalar function.\n\nSee also reducedim.\n\n\n\n\n\n"
},

{
    "location": "api/#JuliaDB.loadndsparse-Tuple{Union{String, AbstractArray{T,1} where T}}",
    "page": "API",
    "title": "JuliaDB.loadndsparse",
    "category": "method",
    "text": "loadndsparse(files::Union{AbstractVector,String}; <options>)\n\nLoad an NDSparse from CSV files.\n\nfiles is either a vector of file paths, or a directory name.\n\nOptions:\n\nindexcols::Vector – columns to use as indexed columns. (by default a 1:n implicit index is used.)\ndatacols::Vector – non-indexed columns. (defaults to all columns but indexed columns). Specify this to only load a subset of columns. In place of the name of a column, you can specify a tuple of names – this will treat any column with one of those names as the same column, but use the first name in the tuple. This is useful when the same column changes name between CSV files. (e.g. vendor_id and VendorId)\n\nAll other options are identical to those in loadtable\n\n\n\n\n\n"
},

{
    "location": "api/#JuliaDB.loadtable-Tuple{Union{String, AbstractArray{T,1} where T}}",
    "page": "API",
    "title": "JuliaDB.loadtable",
    "category": "method",
    "text": "loadtable(files::Union{AbstractVector,String}; <options>)\n\nLoad a table from CSV files.\n\nfiles is either a vector of file paths, or a directory name.\n\nOptions:\n\noutput::AbstractString – directory name to write the table to. By default data is loaded directly to memory. Specifying this option will allow you to load data larger than the available memory.\nindexcols::Vector – columns to use as primary key columns. (defaults to [])\ndatacols::Vector – non-indexed columns. (defaults to all columns but indexed columns). Specify this to only load a subset of columns. In place of the name of a column, you can specify a tuple of names – this will treat any column with one of those names as the same column, but use the first name in the tuple. This is useful when the same column changes name between CSV files. (e.g. vendor_id and VendorId)\ndistributed::Bool – should the output dataset be loaded as a distributed table? If true, this will use all available worker processes to load the data. (defaults to true if workers are available, false if not)\nchunks::Int – number of chunks to create when loading distributed. (defaults to number of workers)\ndelim::Char – the delimiter character. (defaults to ,). Use spacedelim=true to split by spaces.\nspacedelim::Bool: parse space-delimited files. delim has no effect if true.\nquotechar::Char – quote character. (defaults to \")\nescapechar::Char – escape character. (defaults to \")\nfilenamecol::Union{Symbol, Pair} – create a column containing the file names from where each row came from. This argument gives a name to the column. By default, basename(name) of the name is kept, and \".csv\" suffix will be stripped. To provide a custom function to apply on the names, use a name => Function pair. By default, no file name column will be created.\nheader_exists::Bool – does header exist in the files? (defaults to true)\ncolnames::Vector{String} – specify column names for the files, use this with (header_exists=false, otherwise first row is discarded). By default column names are assumed to be present in the file.\nsamecols – a vector of tuples of strings where each tuple contains alternative names for the same column. For example, if some files have the name \"vendorid\" and others have the name \"VendorID\", pass `samecols=[(\"VendorID\", \"vendorid\")]`.\ncolparsers – either a vector or dictionary of data types or an AbstractToken object from TextParse package. By default, these are inferred automatically. See type_detect_rows option below.\ntype_detect_rows: number of rows to use to infer the initial colparsers defaults to 20.\nnastrings::Vector{String} – strings that are to be considered missing values. (defaults to TextParse.NA_STRINGS)\nskiplines_begin::Char – skip some lines in the beginning of each file. (doesn\'t skip by default)\nusecache::Bool: (vestigial)\n\n\n\n\n\n"
},

{
    "location": "api/#JuliaDB.partitionplot",
    "page": "API",
    "title": "JuliaDB.partitionplot",
    "category": "function",
    "text": "partitionplot(table, y;    stat=Extrema(), nparts=100, by=nothing, dropmissing=false)\npartitionplot(table, x, y; stat=Extrema(), nparts=100, by=nothing, dropmissing=false)\n\nPlot a summary of variable y against x (1:length(y) if not specified).  Using nparts approximately-equal sections along the x-axis, the data in y over each section is  summarized by stat. \n\n\n\n\n\n"
},

{
    "location": "api/#JuliaDB.rechunk",
    "page": "API",
    "title": "JuliaDB.rechunk",
    "category": "function",
    "text": "rechunk(t::Union{DNDSparse, DNDSparse}[, by[, select]]; <options>)\n\nReindex and sort a distributed dataset by keys selected by by.\n\nOptionally select specifies which non-indexed fields are kept. By default this is all fields not mentioned in by for Table and the value columns for NDSparse.\n\nOptions:\n\nchunks – how to distribute the data. This can be:\nAn integer – number of chunks to create\nAn vector of k integers – number of elements in each of the k chunks. sum(k) must be same as length(t)\nThe distribution of another array. i.e. vec.subdomains where vec is a distributed array.\nmerge::Function – a function which merges two sub-table or sub-ndsparse into one NDSparse. They may have overlaps in their indices.\nsplitters::AbstractVector – specify keys to split by. To create n chunks you would need to pass n-1 splitters and also the chunks=n option.\nchunks_sorted::Bool – are the chunks sorted locally? If true, this skips sorting or re-indexing them.\naffinities::Vector{<:Integer} – which processes (Int pid) should each output chunk be created on. If unspecified all workers are used.\nclosed::Bool – if true, the same key will not be present in multiple chunks (although sorted). true by default.\nnsamples::Integer – number of keys to randomly sample from each chunk to estimate splitters in the sorting process. (See samplesort). Defaults to 2000.\nbatchsize::Integer – how many chunks at a time from the input should be loaded into memory at any given time. This will essentially sort in batches of batchsize chunks.\n\n\n\n\n\n"
},

{
    "location": "api/#JuliaDB.tracktime-Tuple{Any}",
    "page": "API",
    "title": "JuliaDB.tracktime",
    "category": "method",
    "text": "tracktime(f)\n\nTrack the time spent on different processes in different categories in running f.\n\n\n\n\n\n"
},

{
    "location": "api/#JuliaDB.DIndexedTable",
    "page": "API",
    "title": "JuliaDB.DIndexedTable",
    "category": "type",
    "text": "A distributed table\n\n\n\n\n\n"
},

{
    "location": "api/#JuliaDB.DNDSparse",
    "page": "API",
    "title": "JuliaDB.DNDSparse",
    "category": "type",
    "text": "DNDSparse{K,V} <: AbstractNDSparse\n\nA distributed NDSparse datastructure. Can be constructed by:\n\nndsparse from Julia objects\nloadndsparse from data on disk\ndistribute from an NDSparse object\n\n\n\n\n\n"
},

{
    "location": "api/#JuliaDB.IndexSpace",
    "page": "API",
    "title": "JuliaDB.IndexSpace",
    "category": "type",
    "text": "IndexSpace(interval, boundingrect, nrows)\n\nMetadata about an chunk.\n\ninterval: An Interval object with the first and the last index tuples.\nboundingrect: An Interval object with the lowest and the highest indices as tuples.\nnrows: A Nullable{Int} of number of rows in the NDSparse, if knowable.\n\n\n\n\n\n"
},

{
    "location": "api/#JuliaDB.Interval",
    "page": "API",
    "title": "JuliaDB.Interval",
    "category": "type",
    "text": "An interval type tailored specifically to store intervals of indices of an NDSparse object. Some of the operations on this like in or < may be controversial for a generic Interval type.\n\n\n\n\n\n"
},

{
    "location": "api/#Base.collect-Tuple{JuliaDB.DNDSparse}",
    "page": "API",
    "title": "Base.collect",
    "category": "method",
    "text": "collect(t::DNDSparse)\n\nGets distributed data in a DNDSparse t and merges it into NDSparse object\n\nwarning: Warning\ncollect(t) requires at least as much memory as the size of the result of the computing t. If the result is expected to be big, try compute(save(t, \"output_dir\")) instead. See save for more. This data can be loaded later using load.\n\n\n\n\n\n"
},

{
    "location": "api/#Base.getindex-Union{Tuple{K}, Tuple{DNDSparse{K,V} where V,Vararg{Any,N} where N}} where K",
    "page": "API",
    "title": "Base.getindex",
    "category": "method",
    "text": "t[idx...]\n\nReturns a DNDSparse containing only the elements of t where the given indices (idx) match. If idx has the same type as the index tuple of the t, then this is considered a scalar indexing (indexing of a single value). In this case the value itself is looked up and returned.\n\n\n\n\n\n"
},

{
    "location": "api/#Base.length-Tuple{JuliaDB.DNDSparse}",
    "page": "API",
    "title": "Base.length",
    "category": "method",
    "text": "The length of the DNDSparse if it can be computed. Will throw an error if not. You can get the length of such tables after calling compute on them.\n\n\n\n\n\n"
},

{
    "location": "api/#Base.map-Tuple{Any,JuliaDB.DNDSparse}",
    "page": "API",
    "title": "Base.map",
    "category": "method",
    "text": "map(f, t::DNDSparse)\n\nApplies a function f on every element in the data of table t.\n\n\n\n\n\n"
},

{
    "location": "api/#JuliaDB.fromchunks-Tuple{AbstractArray,Vararg{Any,N} where N}",
    "page": "API",
    "title": "JuliaDB.fromchunks",
    "category": "method",
    "text": "fromchunks(cs)\n\nConstruct a distributed object from chunks. Calls fromchunks(T, cs) where T is the type of the data in the first chunk. Computes any thunks.\n\n\n\n\n\n"
},

{
    "location": "api/#JuliaDB.mapchunks-Union{Tuple{V}, Tuple{K}, Tuple{Any,DNDSparse{K,V}}} where V where K",
    "page": "API",
    "title": "JuliaDB.mapchunks",
    "category": "method",
    "text": "mapchunks(f, t::DNDSparse; keeplengths=true)\n\nApplies a function to each chunk in t. Returns a new DNDSparse. If keeplength is false, this means that the lengths of the output chunks is unknown before compute. This function is used internally by many DNDSparse operations.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.All",
    "page": "API",
    "title": "IndexedTables.All",
    "category": "type",
    "text": "All(cols::Union{Symbol, Int}...)\n\nSelect the union of the selections in cols. If cols == (), select all columns.\n\nExamples\n\nt = table([1,1,2,2], [1,2,1,2], [1,2,3,4], [0, 0, 0, 0], names=[:a,:b,:c,:d])\nselect(t, All(:a, (:b, :c)))\nselect(t, All())\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.Between",
    "page": "API",
    "title": "IndexedTables.Between",
    "category": "type",
    "text": "Between(first, last)\n\nSelect the columns between first and last.\n\nExamples\n\nt = table([1,1,2,2], [1,2,1,2], 1:4, \'a\':\'d\', names=[:a,:b,:c,:d])\nselect(t, Between(:b, :d))\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.ColDict-Tuple{Any}",
    "page": "API",
    "title": "IndexedTables.ColDict",
    "category": "method",
    "text": "d = ColDict(t)\n\nCreate a mutable dictionary of columns in t.\n\nTo get the immutable iterator of the same type as t call d[]\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.IndexedTable",
    "page": "API",
    "title": "IndexedTables.IndexedTable",
    "category": "type",
    "text": "A tabular data structure that extends Columns.  Create an IndexedTable with the  table function.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.Keys",
    "page": "API",
    "title": "IndexedTables.Keys",
    "category": "type",
    "text": "Keys()\n\nSelect the primary keys.\n\nExamples\n\nt = table([1,1,2,2], [1,2,1,2], [1,2,3,4], names=[:a,:b,:c], pkey = (:a, :b))\nselect(t, Keys())\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.NDSparse-Tuple",
    "page": "API",
    "title": "IndexedTables.NDSparse",
    "category": "method",
    "text": "NDSparse(columns...; names=Symbol[...], kwargs...)\n\nConstruct an NDSparse array from columns. The last argument is the data column, and the rest are index columns. The names keyword argument optionally specifies names for the index columns (dimensions).\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.Not",
    "page": "API",
    "title": "IndexedTables.Not",
    "category": "type",
    "text": "Not(cols::Union{Symbol, Int}...)\n\nSelect the complementary of the selection in cols. Not can accept several arguments, in which case it returns the complementary of the union of the selections.\n\nExamples\n\nt = table([1,1,2,2], [1,2,1,2], [1,2,3,4], names=[:a,:b,:c], pkey = (:a, :b))\nselect(t, Not(:a))\nselect(t, Not(:a, (:a, :b)))\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.aggregate!-Tuple{Any,NDSparse}",
    "page": "API",
    "title": "IndexedTables.aggregate!",
    "category": "method",
    "text": "aggregate!(f::Function, arr::NDSparse)\n\nCombine adjacent rows with equal indices using the given 2-argument reduction function, in place.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.asofjoin-Tuple{NDSparse,NDSparse}",
    "page": "API",
    "title": "IndexedTables.asofjoin",
    "category": "method",
    "text": "asofjoin(left::NDSparse, right::NDSparse)\n\nJoin rows from left with the \"most recent\" value from right.\n\nExample\n\nusing Dates\nakey1 = [\"A\", \"A\", \"B\", \"B\"]\nakey2 = [Date(2017,11,11), Date(2017,11,12), Date(2017,11,11), Date(2017,11,12)]\navals = collect(1:4)\n\nbkey1 = [\"A\", \"A\", \"B\", \"B\"]\nbkey2 = [Date(2017,11,12), Date(2017,11,13), Date(2017,11,10), Date(2017,11,13)]\nbvals = collect(5:8)\n\na = ndsparse((akey1, akey2), avals)\nb = ndsparse((bkey1, bkey2), bvals)\n\nasofjoin(a, b)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.collect_columns-Tuple{Any}",
    "page": "API",
    "title": "IndexedTables.collect_columns",
    "category": "method",
    "text": "collect_columns(itr)\n\nCollect an iterable as a Columns object if it iterates Tuples or NamedTuples, as a normal Array otherwise.\n\nExamples\n\ns = [(1,2), (3,4)]\ncollect_columns(s)\n\ns2 = Iterators.filter(isodd, 1:8)\ncollect_columns(s2)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.colnames",
    "page": "API",
    "title": "IndexedTables.colnames",
    "category": "function",
    "text": "colnames(itr)\n\nReturns the names of the \"columns\" in itr.\n\nExamples:\n\ncolnames(1:3)\ncolnames(Columns([1,2,3], [3,4,5]))\ncolnames(table([1,2,3], [3,4,5]))\ncolnames(Columns(x=[1,2,3], y=[3,4,5]))\ncolnames(table([1,2,3], [3,4,5], names=[:x,:y]))\ncolnames(ndsparse(Columns(x=[1,2,3]), Columns(y=[3,4,5])))\ncolnames(ndsparse(Columns(x=[1,2,3]), [3,4,5]))\ncolnames(ndsparse(Columns(x=[1,2,3]), [3,4,5]))\ncolnames(ndsparse(Columns([1,2,3], [4,5,6]), Columns(x=[6,7,8])))\ncolnames(ndsparse(Columns(x=[1,2,3]), Columns([3,4,5],[6,7,8])))\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.columns",
    "page": "API",
    "title": "IndexedTables.columns",
    "category": "function",
    "text": "columns(itr, select::Selection = All())\n\nSelect one or more columns from an iterable of rows as a tuple of vectors.\n\nselect specifies which columns to select. Refer to the select function for the  available selection options and syntax.\n\nitr can be NDSparse, Columns, AbstractVector, or their distributed counterparts.\n\nExamples\n\nt = table(1:2, 3:4; names = [:x, :y])\n\ncolumns(t)\ncolumns(t, :x)\ncolumns(t, (:x,))\ncolumns(t, (:y, :x => -))\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.columns-Tuple{Any,Any}",
    "page": "API",
    "title": "IndexedTables.columns",
    "category": "method",
    "text": "columns(itr, which)\n\nReturns a vector or a tuple of vectors from the iterator.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.convertdim-Tuple{NDSparse,Union{Int64, Symbol},Any}",
    "page": "API",
    "title": "IndexedTables.convertdim",
    "category": "method",
    "text": "convertdim(x::NDSparse, d::DimName, xlate; agg::Function, vecagg::Function, name)\n\nApply function or dictionary xlate to each index in the specified dimension. If the mapping is many-to-one, agg or vecagg is used to aggregate the results. If agg is passed, it is used as a 2-argument reduction function over the data. If vecagg is passed, it is used as a vector-to-scalar function to aggregate the data. name optionally specifies a new name for the translated dimension.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.dimlabels-Tuple{NDSparse}",
    "page": "API",
    "title": "IndexedTables.dimlabels",
    "category": "method",
    "text": "dimlabels(t::NDSparse)\n\nReturns an array of integers or symbols giving the labels for the dimensions of t. ndims(t) == length(dimlabels(t)).\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.dropmissing",
    "page": "API",
    "title": "IndexedTables.dropmissing",
    "category": "function",
    "text": "dropmissing(t        )\ndropmissing(t, select)\n\nDrop rows of table t which contain missing values (either Missing or DataValue),  optionally only using the columns in select.  Column types will be converted to  non-missing types.  For example:\n\nVector{Union{Int, Missing}} -> Vector{Int}\nDataValueArray{Int} -> Vector{Int}\n\nExample\n\nt = table([0.1,0.5,missing,0.7], [2,missing,4,5], [missing,6,missing,7], names=[:t,:x,:y])\ndropmissing(t)\ndropmissing(t, (:t, :x))\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.flatten",
    "page": "API",
    "title": "IndexedTables.flatten",
    "category": "function",
    "text": "flatten(t::Table, col=length(columns(t)))\n\nFlatten col column which may contain a vector of vectors while repeating the other fields. If column argument is not provided, default to last column.\n\nExamples:\n\nx = table([1,2], [[3,4], [5,6]], names=[:x, :y])\nflatten(x, 2)\n\nt1 = table([3,4],[5,6], names=[:a,:b])\nt2 = table([7,8], [9,10], names=[:a,:b])\nx = table([1,2], [t1, t2], names=[:x, :y]);\nflatten(x, :y)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.flush!-Tuple{NDSparse}",
    "page": "API",
    "title": "IndexedTables.flush!",
    "category": "method",
    "text": "flush!(arr::NDSparse)\n\nCommit queued assignment operations, by sorting and merging the internal temporary buffer.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.groupby",
    "page": "API",
    "title": "IndexedTables.groupby",
    "category": "function",
    "text": "groupby(f, t, by = pkeynames(t); select, flatten=false)\n\nApply f to the select-ed columns (see select) in groups defined by the  unique values of by. \n\nIf f returns a vector, split it into multiple columns with flatten = true.\n\nExamples\n\nusing Statistics\n\nt=table([1,1,1,2,2,2], [1,1,2,2,1,1], [1,2,3,4,5,6], names=[:x,:y,:z])\n\ngroupby(mean, t, :x, select=:z)\ngroupby(identity, t, (:x, :y), select=:z)\ngroupby(mean, t, (:x, :y), select=:z)\n\ngroupby((mean, std, var), t, :y, select=:z)\ngroupby((q25=z->quantile(z, 0.25), q50=median, q75=z->quantile(z, 0.75)), t, :y, select=:z)\n\n# apply different aggregation functions to different columns\ngroupby((ymean = :y => mean, zmean = :z => mean), t, :x)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.groupjoin-Tuple{Union{IndexedTable, NDSparse},Union{IndexedTable, NDSparse}}",
    "page": "API",
    "title": "IndexedTables.groupjoin",
    "category": "method",
    "text": "groupjoin(left, right; kw...)\ngroupjoin(f, left, right; kw...)\n\nJoin left and right creating groups of values with matching keys.\n\nFor keyword argument options, see join.\n\nExamples\n\nl = table([1,1,1,2], [1,2,2,1], [1,2,3,4], names=[:a,:b,:c], pkey=(:a, :b))\nr = table([0,1,1,2], [1,2,2,1], [1,2,3,4], names=[:a,:b,:d], pkey=(:a, :b))\n\ngroupjoin(l, r)\ngroupjoin(l, r; how = :left)\ngroupjoin(l, r; how = :outer)\ngroupjoin(l, r; how = :anti)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.groupreduce",
    "page": "API",
    "title": "IndexedTables.groupreduce",
    "category": "function",
    "text": "groupreduce(f, t, by = pkeynames(t); select)\n\nCalculate a reduce operation f over table t on groups defined by the values  in selection by.  The result is put in a table keyed by the unique by values.\n\nExamples\n\nt = table([1,1,1,2,2,2], 1:6, names=[:x, :y])\ngroupreduce(+,        t, :x; select = :y)\ngroupreduce((sum=+,), t, :x; select = :y)  # change output column name to :sum\n\nt2 = table([1,1,1,2,2,2], [1,1,2,2,3,3], 1:6, names = [:x, :y, :z])\ngroupreduce(+, t2, (:x, :y), select = :z)\n\n# different reducers for different columns\ngroupreduce((sumy = :y => +, sumz = :z => +), t2, :x)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.insertcol-Tuple{Any,Integer,Any,Any}",
    "page": "API",
    "title": "IndexedTables.insertcol",
    "category": "method",
    "text": "insertcol(t, position::Integer, name, x)\n\nInsert a column x named name at position. Returns a new table.\n\nExample\n\nt = table([0.01, 0.05], [2,1], [3,4], names=[:t, :x, :y], pkey=:t)\ninsertcol(t, 2, :w, [0,1])\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.insertcolafter-NTuple{4,Any}",
    "page": "API",
    "title": "IndexedTables.insertcolafter",
    "category": "method",
    "text": "insertcolafter(t, after, name, col)\n\nInsert a column col named name after after. Returns a new table.\n\nExample\n\nt = table([0.01, 0.05], [2,1], [3,4], names=[:t, :x, :y], pkey=:t)\ninsertcolafter(t, :t, :w, [0,1])\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.insertcolbefore-NTuple{4,Any}",
    "page": "API",
    "title": "IndexedTables.insertcolbefore",
    "category": "method",
    "text": "insertcolbefore(t, before, name, col)\n\nInsert a column col named name before before. Returns a new table.\n\nExample\n\nt = table([0.01, 0.05], [2,1], [3,4], names=[:t, :x, :y], pkey=:t)\ninsertcolbefore(t, :x, :w, [0,1])\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.map_rows-Tuple{Any,Vararg{Any,N} where N}",
    "page": "API",
    "title": "IndexedTables.map_rows",
    "category": "method",
    "text": "map_rows(f, c...)\n\nTransform collection c by applying f to each element. For multiple collection arguments, apply f elementwise. Collect output as Columns if f returns Tuples or NamedTuples with constant fields, as Array otherwise.\n\nExamples\n\nmap_rows(i -> (exp = exp(i), log = log(i)), 1:5)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.ncols",
    "page": "API",
    "title": "IndexedTables.ncols",
    "category": "function",
    "text": "ncols(itr)\n\nReturns the number of columns in itr.\n\nExamples\n\nncols([1,2,3]) == 1\nncols(rows(([1,2,3],[4,5,6]))) == 2\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.ndsparse",
    "page": "API",
    "title": "IndexedTables.ndsparse",
    "category": "function",
    "text": "ndsparse(keys, values; kw...)\n\nConstruct an NDSparse array with the given keys and values columns. On construction,  the keys and data are sorted in lexicographic order of the keys.\n\nKeyword Argument Options:\n\nagg = nothing – Function to aggregate values with duplicate keys.\npresorted = false – Are the key columns already sorted?\ncopy = true – Should the columns in keys and values be copied?\nchunks = nothing – Provide an integer to distribute data into chunks chunks.\nA good choice is nworkers() (after using Distributed)\nSee also: distribute\n\nExamples:\n\nx = ndsparse([\"a\",\"b\"], [3,4])\nkeys(x)\nvalues(x)\nx[\"a\"]\n\n# Dimensions are named if constructed with a named tuple of columns \nx = ndsparse((index = 1:10,), rand(10))\nx[1]\n\n# Multiple dimensions by passing a (named) tuple of columns\nx = ndsparse((x = 1:10, y = 1:2:20), rand(10))\nx[1, 1]\n\n# Value columns can also have names via named tuples\nx = ndsparse(1:10, (x=rand(10), y=rand(10)))\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.pkeynames-Tuple{IndexedTables.AbstractIndexedTable}",
    "page": "API",
    "title": "IndexedTables.pkeynames",
    "category": "method",
    "text": "pkeynames(t::Table)\n\nNames of the primary key columns in t.\n\nExamples\n\nt = table([1,2], [3,4]);\npkeynames(t)\n\nt = table([1,2], [3,4], pkey=1);\npkeynames(t)\n\nt = table([2,1],[1,3],[4,5], names=[:x,:y,:z], pkey=(1,2));\npkeynames(t)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.pkeynames-Tuple{NDSparse}",
    "page": "API",
    "title": "IndexedTables.pkeynames",
    "category": "method",
    "text": "pkeynames(t::NDSparse)\n\nNames of the primary key columns in t.\n\nExample\n\nx = ndsparse([1,2],[3,4])\npkeynames(x)\n\nx = ndsparse((x=1:10, y=1:2:20), rand(10))\npkeynames(x)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.pkeys-Tuple{IndexedTable}",
    "page": "API",
    "title": "IndexedTables.pkeys",
    "category": "method",
    "text": "pkeys(itr::IndexedTable)\n\nPrimary keys of the table. If Table doesn\'t have any designated primary key columns (constructed without pkey argument) then a default key of tuples (1,):(n,) is generated.\n\nExample\n\na = table([\"a\",\"b\"], [3,4]) # no pkey\npkeys(a)\n\na = table([\"a\",\"b\"], [3,4], pkey=1)\npkeys(a)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.popcol-Tuple{Any,Vararg{Any,N} where N}",
    "page": "API",
    "title": "IndexedTables.popcol",
    "category": "method",
    "text": "popcol(t, cols...)\n\nRemove the column(s) cols from the table. Returns a new table.\n\nExample\n\nt = table([0.01, 0.05], [2,1], [3,4], names=[:t, :x, :y], pkey=:t)\npopcol(t, :x)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.pushcol-Tuple{Any,Vararg{Any,N} where N}",
    "page": "API",
    "title": "IndexedTables.pushcol",
    "category": "method",
    "text": "pushcol(t, name, x)\n\nPush a column x to the end of the table. name is the name for the new column. Returns a new table.\n\npushcol(t, map::Pair...)\n\nPush many columns at a time.\n\nExample\n\nt = table([0.01, 0.05], [2,1], [3,4], names=[:t, :x, :y], pkey=:t)\npushcol(t, :z, [1//2, 3//4])\npushcol(t, :z => [1//2, 3//4])\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.reducedim_vec-Tuple{Any,NDSparse,Any}",
    "page": "API",
    "title": "IndexedTables.reducedim_vec",
    "category": "method",
    "text": "reducedim_vec(f::Function, arr::NDSparse, dims)\n\nLike reduce, except uses a function mapping a vector of values to a scalar instead of a 2-argument scalar function.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.reindex",
    "page": "API",
    "title": "IndexedTables.reindex",
    "category": "function",
    "text": "reindex(t::IndexedTable, by)\nreindex(t::IndexedTable, by, select)\n\nReindex table t with new primary key by, optionally keeping a subset of columns via select.  For NDSparse, use selectkeys.\n\nExample\n\nt = table([2,1],[1,3],[4,5], names=[:x,:y,:z], pkey=(1,2))\n\nt2 = reindex(t, (:y, :z))\n\npkeynames(t2)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.renamecol-Tuple{Any,Vararg{Any,N} where N}",
    "page": "API",
    "title": "IndexedTables.renamecol",
    "category": "method",
    "text": "renamecol(t, col, newname)\n\nSet newname as the new name for column col in t. Returns a new table.\n\nrenamecol(t, map::Pair...)\n\nRename multiple columns at a time.\n\nExample\n\nt = table([0.01, 0.05], [2,1], names=[:t, :x])\nrenamecol(t, :t, :time)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.rows",
    "page": "API",
    "title": "IndexedTables.rows",
    "category": "function",
    "text": "rows(itr, select = All())\n\nSelect one or more fields from an iterable of rows as a vector of their values.  Refer to  the select function for selection options and syntax.\n\nitr can be NDSparse, Columns, AbstractVector, or their distributed counterparts.\n\nExamples\n\nt = table([1,2],[3,4], names=[:x,:y])\nrows(t)\nrows(t, :x)\nrows(t, (:x,))\nrows(t, (:y, :x => -))\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.select-Tuple{IndexedTables.AbstractIndexedTable,Any}",
    "page": "API",
    "title": "IndexedTables.select",
    "category": "method",
    "text": "select(t::Table, which::Selection)\n\nSelect all or a subset of columns, or a single column from the table.\n\nSelection is a type union of many types that can select from a table. It can be:\n\nInteger – returns the column at this position.\nSymbol – returns the column with this name.\nPair{Selection => Function} – selects and maps a function over the selection, returns the result.\nAbstractArray – returns the array itself. This must be the same length as the table.\nTuple of Selection – returns a table containing a column for every selector in the tuple. The tuple may also contain the type Pair{Symbol, Selection}, which the selection a name. The most useful form of this when introducing a new column.\nRegex – returns the columns with names that match the regular expression.\nType – returns columns with elements of the given type.\n\nExamples:\n\nt = table(1:10, randn(10), rand(Bool, 10); names = [:x, :y, :z])\n\n# select the :x vector\nselect(t, 1)\nselect(t, :x)\n\n# map a function to the :y vector\nselect(t, 2 => abs)\nselect(t, :y => x -> x > 0 ? x : -x)\n\n# select the table of :x and :z\nselect(t, (:x, :z))\nselect(t, r\"(x|z)\")\n\n# map a function to the table of :x and :y\nselect(t, (:x, :y) => row -> row[1] + row[2])\nselect(t, (1, :y) => row -> row.x + row.y)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.selectkeys-Tuple{NDSparse,Any}",
    "page": "API",
    "title": "IndexedTables.selectkeys",
    "category": "method",
    "text": "selectkeys(x::NDSparse, sel)\n\nReturn an NDSparse with a subset of keys.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.selectvalues-Tuple{NDSparse,Any}",
    "page": "API",
    "title": "IndexedTables.selectvalues",
    "category": "method",
    "text": "selectvalues(x::NDSparse, sel)\n\nReturn an NDSparse with a subset of values\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.setcol-Tuple{Any,Vararg{Any,N} where N}",
    "page": "API",
    "title": "IndexedTables.setcol",
    "category": "method",
    "text": "setcol(t::Table, col::Union{Symbol, Int}, x::Selection)\n\nSets a x as the column identified by col. Returns a new table.\n\nsetcol(t::Table, map::Pair{}...)\n\nSet many columns at a time.\n\nExamples:\n\nt = table([1,2], [3,4], names=[:x, :y])\n\n# change second column to [5,6]\nsetcol(t, 2 => [5,6])\nsetcol(t, :y , :y => x -> x + 2)\n\n# add [5,6] as column :z \nsetcol(t, :z => 5:6)\nsetcol(t, :z, :y => x -> x + 2)\n\n# replacing the primary key results in a re-sorted copy\nt = table([0.01, 0.05], [1,2], [3,4], names=[:t, :x, :y], pkey=:t)\nt2 = setcol(t, :t, [0.1,0.05])\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.stack-Union{Tuple{D}, Tuple{D}, Tuple{D,Any}} where D<:Union{IndexedTable, NDSparse}",
    "page": "API",
    "title": "IndexedTables.stack",
    "category": "method",
    "text": "stack(t, by = pkeynames(t); select = Not(by), variable = :variable, value = :value)`\n\nReshape a table from the wide to the long format. Columns in by are kept as indexing columns. Columns in select are stacked. In addition to the id columns, two additional columns labeled  variable and value are added, containing the column identifier and the stacked columns. See also unstack.\n\nExamples\n\nt = table(1:4, names = [:x], pkey=:x)\nt = pushcol(t, :xsquare, :x => x -> x^2)\nt = pushcol(t, :xcube  , :x => x -> x^3)\n\nstack(t)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.summarize",
    "page": "API",
    "title": "IndexedTables.summarize",
    "category": "function",
    "text": "summarize(f, t, by = pkeynames(t); select = Not(by), stack = false, variable = :variable)\n\nApply summary functions column-wise to a table. Return a NamedTuple in the non-grouped case and a table in the grouped case. Use stack=true to stack results of the same summary function  for different columns.\n\nExamples\n\nusing Statistics\n\nt = table([1, 2, 3], [1, 1, 1], names = [:x, :y])\n\nsummarize((mean, std), t)\nsummarize((m = mean, s = std), t)\nsummarize(mean, t; stack=true)\nsummarize((mean, std), t; select = :y)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.table",
    "page": "API",
    "title": "IndexedTables.table",
    "category": "function",
    "text": "table(cols; kw...)\n\nCreate a table from a (named) tuple of AbstractVectors.\n\ntable(cols::AbstractVector...; names::Vector{Symbol}, kw...)\n\nCreate a table from the provided cols, optionally with names.\n\ntable(cols::Columns; kw...)\n\nConstruct a table from a vector of tuples. See rows and Columns.\n\ntable(t::Union{IndexedTable, NDSparse}; kw...)\n\nCopy a Table or NDSparse to create a new table. The same primary keys as the input are used.\n\ntable(x; kw...)\n\nCreate an IndexedTable from any object x that follows the Tables.jl interface.\n\nKeyword Argument Options:\n\npkey: select columns to sort by and be the primary key.\npresorted = false: is the data pre-sorted by primary key columns? \ncopy = true: creates a copy of the input vectors if true. Irrelevant if chunks is specified.\nchunks::Integer: distribute the table.  Options are:\nInt – (number of chunks) a safe bet is nworkers() after using Distributed.\nVector{Int} – Number of elements in each of the length(chunks) chunks.\n\nExamples:\n\ntable(rand(10), rand(10), names = [:x, :y], pkey = :x)\n\ntable(rand(Bool, 20), rand(20), rand(20), pkey = [1,2])\n\ntable((x = 1:10, y = randn(10)))\n\ntable([(1,2), (3,4)])\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.unstack-Union{Tuple{D}, Tuple{D}, Tuple{D,Any}} where D<:Union{IndexedTable, NDSparse}",
    "page": "API",
    "title": "IndexedTables.unstack",
    "category": "method",
    "text": "unstack(t, by = pkeynames(t); variable = :variable, value = :value)\n\nReshape a table from the long to the wide format. Columns in by are kept as indexing columns. Keyword arguments variable and value denote which column contains the column identifier and which the corresponding values.  See also stack.\n\nExamples\n\nt = table(1:4, [1, 4, 9, 16], [1, 8, 27, 64], names = [:x, :xsquare, :xcube], pkey = :x);\n\nlong = stack(t)\n\nunstack(long)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.update!-Union{Tuple{N}, Tuple{Union{Function, Type},NDSparse,Vararg{Any,N}}} where N",
    "page": "API",
    "title": "IndexedTables.update!",
    "category": "method",
    "text": "update!(f::Function, arr::NDSparse, indices...)\n\nReplace data values x with f(x) at each location that matches the given indices.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.where-Union{Tuple{N}, Tuple{NDSparse,Vararg{Any,N}}} where N",
    "page": "API",
    "title": "IndexedTables.where",
    "category": "method",
    "text": "where(arr::NDSparse, indices...)\n\nReturns an iterator over data items where the given indices match. Accepts the same index arguments as getindex.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.Perm",
    "page": "API",
    "title": "IndexedTables.Perm",
    "category": "type",
    "text": "A permutation\n\nFields:\n\ncolumns: The columns being indexed as a vector of integers (column numbers)\nperm: the permutation - an array or iterator which has the sorted permutation\n\n\n\n\n\n"
},

{
    "location": "api/#Base.Broadcast.broadcast-Tuple{Function,NDSparse,NDSparse}",
    "page": "API",
    "title": "Base.Broadcast.broadcast",
    "category": "method",
    "text": "broadcast(f, A::NDSparse, B::NDSparse; dimmap::Tuple{Vararg{Int}})\nA .* B\n\nCompute an inner join of A and B using function f, where the dimensions of B are a subset of the dimensions of A. Values from B are repeated over the extra dimensions.\n\ndimmap optionally specifies how dimensions of A correspond to dimensions of B. It is a tuple where dimmap[i]==j means the ith dimension of A matches the jth dimension of B. Extra dimensions that do not match any dimensions of j should have dimmap[i]==0.\n\nIf dimmap is not specified, it is determined automatically using index column names and types.\n\nExample\n\na = ndsparse(([1,1,2,2], [1,2,1,2]), [1,2,3,4])\nb = ndsparse([1,2], [1/1, 1/2])\nbroadcast(*, a, b)\n\ndimmap maps dimensions that should be broadcasted:\n\nbroadcast(*, a, b, dimmap=(0,1))\n\n\n\n\n\n"
},

{
    "location": "api/#Base.convert-Tuple{Type{IndexedTable},Any,Any}",
    "page": "API",
    "title": "Base.convert",
    "category": "method",
    "text": "convert(IndexedTable, pkeys, vals; kwargs...)\n\nConstruct a table with pkeys as primary keys and vals as corresponding non-indexed items. keyword arguments will be forwarded to table constructor.\n\nExample\n\nconvert(IndexedTable, Columns(x=[1,2],y=[3,4]), Columns(z=[1,2]), presorted=true)\n\n\n\n\n\n"
},

{
    "location": "api/#Base.filter-Tuple{Any,Union{IndexedTable, NDSparse}}",
    "page": "API",
    "title": "Base.filter",
    "category": "method",
    "text": "filter(f, t::Union{IndexedTable, NDSparse}; select)\n\nIterate over t and Return the rows for which f(row) returns true.  select determines  the rows that are given as arguments to f (see select).\n\nf can also be a tuple of column => function pairs.  Returned rows will be those for which all conditions are true.\n\nExample\n\n# filter iterates over ROWS of a IndexedTable\nt = table(rand(100), rand(100), rand(100), names = [:x, :y, :z])\nfilter(r -> r.x + r.y + r.z < 1, t)\n\n# filter iterates over VALUES of an NDSparse\nx = ndsparse(1:100, randn(100))\nfilter(val -> val > 0, x)\n\n\n\n\n\n"
},

{
    "location": "api/#Base.join-Tuple{Any,Union{IndexedTable, NDSparse},Union{IndexedTable, NDSparse}}",
    "page": "API",
    "title": "Base.join",
    "category": "method",
    "text": "join(left, right; kw...)\njoin(f, left, right; kw...)\n\nJoin tables left and right.\n\nIf a function f(leftrow, rightrow) is provided, the returned table will have a single  output column.  See the Examples below.\n\nIf the same key occurs multiple times in either table, each left row will get matched  with each right row, resulting in n_occurrences_left * n_occurrences_right output rows.\n\nOptions (keyword arguments)\n\nhow = :inner \nJoin method to use. Described below.\nlkey = pkeys(left) \nFields from left to match on (see pkeys).\nrkey = pkeys(right) \nFields from right to match on.\nlselect = Not(lkey) \nOutput columns from left (see Not)\nrselect = Not(rkey)\nOutput columns from right.\nmissingtype = Missing \nType of missing values that can be created through :left and :outer joins.\nOther supported option is DataValue.\n\nJoin methods (how = :inner)\n\n:inner – rows with matching keys in both tables\n:left – all rows from left, plus matched rows from right (missing values can occur)\n:outer – all rows from both tables (missing values can occur)\n:anti – rows in left WITHOUT matching keys in right\n\nExamples\n\na = table((x = 1:10,   y = rand(10)), pkey = :x)\nb = table((x = 1:2:20, z = rand(10)), pkey = :x)\n\njoin(a, b; how = :inner)\njoin(a, b; how = :left)\njoin(a, b; how = :outer)\njoin(a, b; how = :anti)\n\njoin((l, r) -> l.y + r.z, a, b)\n\n\n\n\n\n"
},

{
    "location": "api/#Base.keys-Tuple{NDSparse,Vararg{Any,N} where N}",
    "page": "API",
    "title": "Base.keys",
    "category": "method",
    "text": "keys(x::NDSparse[, select::Selection])\n\nGet the keys of an NDSparse object. Same as rows but acts only on the index columns of the NDSparse.\n\n\n\n\n\n"
},

{
    "location": "api/#Base.map-Tuple{Any,IndexedTables.AbstractIndexedTable}",
    "page": "API",
    "title": "Base.map",
    "category": "method",
    "text": "map(f, t::IndexedTable; select)\n\nApply f to every item in t selected by select (see also the select function).   Returns a new table if f returns a tuple or named tuple.  If not, returns a vector.\n\nExamples\n\nt = table([1,2], [3,4], names=[:x, :y])\n\npolar = map(p -> (r = hypot(p.x, p.y), θ = atan(p.y, p.x)), t)\n\nback2t = map(p -> (x = p.r * cos(p.θ), y = p.r * sin(p.θ)), polar)\n\n\n\n\n\n"
},

{
    "location": "api/#Base.map-Tuple{Any,NDSparse}",
    "page": "API",
    "title": "Base.map",
    "category": "method",
    "text": "map(f, x::NDSparse; select = values(x))\n\nApply f to every value of select selected from x (see select).\n\nApply f to every data value in x. select selects fields passed to f. By default, the data values are selected.\n\nIf the return value of f is a tuple or named tuple the result will contain many data columns.\n\nExamples\n\nx = ndsparse((t=[0.01, 0.05],), (x=[1,2], y=[3,4]))\n\npolar = map(row -> (r = hypot(row.x, row.y), θ = atan(row.y, row.x)), x)\n\nback2x = map(row -> (x = row.r * cos(row.θ), y = row.r * sin(row.θ)), polar)\n\n\n\n\n\n"
},

{
    "location": "api/#Base.merge-Tuple{Union{IndexedTable, NDSparse},Any}",
    "page": "API",
    "title": "Base.merge",
    "category": "method",
    "text": "merge(a::IndexedTable, b::IndexedTable; pkey)\n\nMerge rows of a with rows of b and remain ordered by the primary key(s).  a and b must have the same column names.\n\nmerge(a::NDSparse, a::NDSparse; agg)\n\nMerge rows of a with rows of b.  To keep unique keys, the value from b takes priority. A provided function agg will aggregate values from a and b that have the same key(s).\n\nExample:\n\na = table((x = 1:5, y = rand(5)); pkey = :x)\nb = table((x = 6:10, y = rand(5)); pkey = :x)\nmerge(a, b)\n\na = ndsparse([1,3,5], [1,2,3])\nb = ndsparse([2,3,4], [4,5,6])\nmerge(a, b)\nmerge(a, b; agg = (x,y) -> x)\n\n\n\n\n\n"
},

{
    "location": "api/#Base.pairs-Union{Tuple{N}, Tuple{NDSparse,Vararg{Any,N}}} where N",
    "page": "API",
    "title": "Base.pairs",
    "category": "method",
    "text": "pairs(arr::NDSparse, indices...)\n\nSimilar to where, but returns an iterator giving index=>value pairs. index will be a tuple.\n\n\n\n\n\n"
},

{
    "location": "api/#Base.reduce-Tuple{Any,IndexedTable}",
    "page": "API",
    "title": "Base.reduce",
    "category": "method",
    "text": "reduce(f, t::IndexedTable; select::Selection)\n\nApply reducer function f pair-wise to the selection select in t.  The reducer f  can be:\n\nA function\nAn OnlineStat\nA (named) tuple of functions and/or OnlineStats\nA (named) tuple of (selector => function) or (selector => OnlineStat) pairs\n\nExamples\n\nt = table(1:5, 6:10, names = [:t, :x])\n\nreduce(+, t, select = :t)\nreduce((a, b) -> (t = a.t + b.t, x = a.x + b.x), t)\n\nusing OnlineStats\nreduce(Mean(), t, select = :t)\nreduce((Mean(), Variance()), t, select = :t)\n\ny = reduce((min, max), t, select=:x)\nreduce((sum = +, prod = *), t, select=:x)\n\n# combining reduction and selection\nreduce((xsum = :x => +, negtsum = (:t => -) => +), t)\n\n\n\n\n\n"
},

{
    "location": "api/#Base.reduce-Tuple{Any,NDSparse}",
    "page": "API",
    "title": "Base.reduce",
    "category": "method",
    "text": "reduce(f, x::NDSparse; dims)\n\nDrop the dims dimension(s) and aggregate values with f.\n\nx = ndsparse((x=[1,1,1,2,2,2],\n              y=[1,2,2,1,2,2],\n              z=[1,1,2,1,1,2]), [1,2,3,4,5,6])\n              \nreduce(+, x; dims=1)\nreduce(+, x; dims=(1,3))\n\n\n\n\n\n"
},

{
    "location": "api/#Base.sort!-Tuple{IndexedTable,Vararg{Any,N} where N}",
    "page": "API",
    "title": "Base.sort!",
    "category": "method",
    "text": "sort!(t    ; kw...)\nsort!(t, by; kw...)\n\nSort rows of t by by in place. All of Base.sort keyword arguments can be used.\n\nExamples\n\nt = table([1,1,1,2,2,2], [1,1,2,2,1,1], [1,2,3,4,5,6], names=[:x,:y,:z]);\nsort!(t, :z, rev = true)\nt\n\n\n\n\n\n"
},

{
    "location": "api/#Base.sort-Tuple{IndexedTable,Vararg{Any,N} where N}",
    "page": "API",
    "title": "Base.sort",
    "category": "method",
    "text": "sort(t    ; select, kw...)\nsort(t, by; select, kw...)\n\nSort rows by by. All of Base.sort keyword arguments can be used.\n\nExamples\n\nt=table([1,1,1,2,2,2], [1,1,2,2,1,1], [1,2,3,4,5,6],\nsort(t, :z; select = (:y, :z), rev = true)\n\n\n\n\n\n"
},

{
    "location": "api/#Base.values-Tuple{NDSparse,Vararg{Any,N} where N}",
    "page": "API",
    "title": "Base.values",
    "category": "method",
    "text": "values(x::NDSparse[, select::Selection])\n\nGet the values of an NDSparse object. Same as rows but acts only on the value columns of the NDSparse.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.arrayof-Tuple{Any}",
    "page": "API",
    "title": "IndexedTables.arrayof",
    "category": "method",
    "text": "arrayof(T)\n\nReturns the type of Columns or Vector suitable to store values of type T. Nested tuples beget nested Columns.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.best_perm_estimate-Tuple{Any,Any}",
    "page": "API",
    "title": "IndexedTables.best_perm_estimate",
    "category": "method",
    "text": "Returns: (n, perm) where n is the number of columns in the beginning of cols, perm is one possible permutation of those first n columns.\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.convertmissing-Tuple{IndexedTable,Type{Missing}}",
    "page": "API",
    "title": "IndexedTables.convertmissing",
    "category": "method",
    "text": "convertmissing(tbl, missingtype)\n\nConvert the missing value representation in tbl to be of type missingtype.\n\nExample\n\nusing IndexedTables, DataValues\nt = table([1,2,missing], [1,missing,3])\nIndexedTables.convertmissing(t, DataValue)\n\n\n\n\n\n"
},

{
    "location": "api/#IndexedTables.excludecols-Tuple{Any,Any}",
    "page": "API",
    "title": "IndexedTables.excludecols",
    "category": "method",
    "text": "excludecols(itr, cols) -> Tuple of Int\n\nNames of all columns in itr except cols. itr can be any of Table, NDSparse, Columns, or AbstractVector\n\nExamples\n\nusing IndexedTables: excludecols\n\nt = table([2,1],[1,3],[4,5], names=[:x,:y,:z], pkey=(1,2))\n\nexcludecols(t, (:x,))\nexcludecols(t, (2,))\nexcludecols(t, pkeynames(t))\nexcludecols([1,2,3], (1,))\n\n\n\n\n\n"
},

{
    "location": "api/#API-1",
    "page": "API",
    "title": "API",
    "category": "section",
    "text": "Modules = [JuliaDB, IndexedTables]"
},

]}
