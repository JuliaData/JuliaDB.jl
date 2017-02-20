using TextParse
using IndexedTables
using Glob

export @dateformat_str, load, csvread, loadNDSparse, glob

function getsubset(cols, subcols)
    idx = length(subcols) > 1 ?
        Columns(map(i -> cols[i], subcols)...) : Columns(cols[subcols[1]])
end

"""
    loadNDSparse(file::AbstractString;
                 indexcols, datacols, agg, presorted, copy, kwargs...)

Load a CSV file into an NDSparse data. `indexcols` (AbstractArray)
specifies which columns form the index of the data, `datacols`
(AbstractArray) specifies which columns are to be used as the data.
`agg`, `presorted`, `copy` options are passed on to `NDSparse`
constructor, any other keyword argument is passed on to `readcsv`
"""
function loadNDSparse(file::AbstractString, delim=',';
                      indexcols=Int[],
                      datacols=-1,
                      agg=nothing,
                      presorted=false,
                      copy=false,
                      kwargs...)

    println("LOADING ", file)
    cols,header = csvread(file, delim; kwargs...)
    if datacols == -1
        # last column
        datacols = length(cols)
    end

    if isempty(indexcols)
        # all columns that aren't data
        indexcols = [x for x in 1:length(cols) if !(x in datacols)]
    end

    NDSparse(getsubset(cols, indexcols), getsubset(cols, datacols))
end

const cache = Dict()
function cached_loadNDSparse(file, delim=','; kwargs...)
    f = abspath(file)
    if haskey(cache, f)
        cache[f]
    else
        cache[f] = loadNDSparse(file, delim; kwargs...)
    end
end

# Get the ChunkInfo from a filename and the data in it
# this is run on the workers
function chunkinfo(file::String, data::NDSparse)
    ChunkInfo(file,
              typeof(data),
              length(data),
              domain(data))
end

comp(f,g) = (x...) -> f(g(x...))

function load(files::AbstractVector; opts...)
    # Give an idea of what we're up against, we should probably also show a
    # progress meter.
    sz = sum(map(filesize, files))
    println("Loading $(length(files)) csv files totalling $(round(sz/2^10)) kB...")

    # Load the data first into memory
    data = map(tothunk(f -> loadNDSparse(f; opts...), persist=true), files)

    chunks = compute(Thunk(data; meta=true) do cs...
            # TODO: this should be read in from Parquet files (saved from step 1)
            # right now we are just caching it in memory...
            [cs...]
        end)

    _DTable(chunks)
end

let
    load(glob("../test/fxsample/*.csv"), header_exists=false, type_detect_rows=4)
end
