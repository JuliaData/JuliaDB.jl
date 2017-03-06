const JULIADB_CACHEDIR = ".juliadb_cache"
const JULIADB_FILECACHE = "filemeta.dat"

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

    if isempty(files)
        throw(ArgumentError("Specify at least one file to load."))
    end

    for file in files
        if !isfile(file)
            throw(ArgumentError("No file named $file"))
        end
    end

    if !isdir(JULIADB_CACHEDIR)
        mkdir(JULIADB_CACHEDIR)
    end

    unknown = files
    validcache = []
    metadata = nothing

    # there can be multiple NDSparse possible in the same file
    # we hope that each one has a unique hash:
    opthash = hash(Dict(opts))
    # Read metadata about a subset of files if safe to
    metafile = joinpath(JULIADB_CACHEDIR, JULIADB_FILECACHE)
    if isfile(metafile)
        try
            metadata = open(deserialize, metafile, "r")
        catch err
            # error reading metadata
            warn("Cached metadata file is corrupt. Not using cache.")
            @goto readunknown
        end
        knownmeta = metadata[files, opthash]
        known = knownmeta.index.columns.filename

        # only those with the same mtime
        valid = knownmeta.data.columns.mtime .== mtime.(known)
        validcache = knownmeta.data.columns.metadata[valid]
        unknown = setdiff(files, known[valid])
    end
    @label readunknown

    # Give an idea of what we're up against, we should probably also show a
    # progress meter.
    println("Metadata for ", length(files)-length(unknown), " / ",
            length(files), " files can be loaded from cache.")

    if isempty(unknown)
        # we read all required metadata from cache
        return fromchunks(validcache)
    end

    sz = sum(map(filesize, unknown))
    println("Reading $(length(unknown)) csv files totalling $(round(sz/2^10)) kB...")
    # Load the data first into memory
    load_f(f) = makecsvchunk(f, delim; opts...)
    data = map(delayed(load_f), unknown)

    chunkrefs = gather(delayed(vcat)(data...))
    # store this back in cache
    cache = NDSparse(Columns(unknown, fill(opthash, length(unknown)), names=[:filename, :opthash]),
                     Columns(mtime.(unknown), Dagger.Chunk[chunkrefs...], names=[:mtime, :metadata]))

    if metadata != nothing
        cache = merge(metadata, cache)
    end
    open(metafile, "w") do io
        serialize(io, cache)
    end

    fromchunks(vcat(validcache, chunkrefs))
end

## TODO: Can make this an LRU cache
const _read_cache = WeakKeyDict()

type CSVChunk
    filename::String
    cache::Bool
    delim::Char
    opts::Dict
end

function gather(ctx, csv::CSVChunk)
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
