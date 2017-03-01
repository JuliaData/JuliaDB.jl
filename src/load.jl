"""
    load(files::AbstractVector;
          indexcols=Int[],
          datacols=Int[],
          agg=nothing,
          presorted=false,
          copy=false,
          csvopts...)

Load a bunch of CSV `files` into a DTable. `indexcols` is a vector of column
indices to be used as the index, and `datacols` is a vector of column indices
to be used as the data for the resulting table. `agg`, `presorted` and `copy`
are the corresponding keyword arguments passed to `NDSparse` constructor.
The rest of the keyword arguments (`csvopts`) will be passed on to `TextParse.csvread`
"""
function load(files::AbstractVector, delim=','; opts...)
    # Give an idea of what we're up against, we should probably also show a
    # progress meter.
    sz = sum(map(filesize, files))
    println("Loading $(length(files)) csv files totalling $(round(sz/2^10)) kB...")

    # Load the data first into memory
    load_f(f) = makecsvchunk(f, delim; opts...)
    data = map(delayed(load_f), files)

    chunkrefs = gather(delayed(vcat)(data...))
    fromchunks(chunkrefs)
end

## TODO: Can make this an LRU cache
const _read_cache = WeakKeyDict()

type CSVChunk <: Dagger.ChunkIO
    filename::String
    cache::Bool
    delim::Char
    opts::Dict
end

function Dagger.gather(ctx, csv::CSVChunk)
    if csv.cache && haskey(_read_cache, csv)
        _read_cache[csv]
    else
        _read_cache[csv] = loadNDSparse(csv.filename, csv.delim; csv.opts...)
    end
end

function makecsvchunk(file, delim; cache=true, opts...)
    handle = CSVChunk(file, cache, delim, Dict(opts))
    # We need to actually load the data to get things like
    # the type and Domain. It will get cached if cache is true
    nds = gather(Context(), handle)
    Dagger.Chunk(typeof(nds), domain(nds), handle, false)
end
