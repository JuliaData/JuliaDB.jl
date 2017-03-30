export loadfiles

const JULIADB_CACHEDIR = ".juliadb_cache"
const JULIADB_FILECACHE = "filemeta.dat"

"""
    loadfiles(files::AbstractVector;
          usecache=true,
          indexcols=Int[],
          datacols=Int[],
          agg=nothing,
          presorted=false,
          copy=false,
          csvopts...)

Load a bunch of CSV `files` into a DTable. `indexcols` is a vector of column
indices to be used as the index, and `datacols` is a vector of column indices
to be used as the data for the resulting table. `agg`, `presorted` and `copy`
are the corresponding keyword arguments passed to `Table` constructor.
The rest of the keyword arguments (`csvopts`) will be passed on to `TextParse.csvread`
"""
function loadfiles(files::AbstractVector, delim=','; usecache=true, opts...)

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

    # there can be multiple Table possible in the same file
    # we hope that each one has a unique hash:
    opthash = hash(Dict(opts))
    # Read metadata about a subset of files if safe to
    metafile = joinpath(JULIADB_CACHEDIR, JULIADB_FILECACHE)
    if usecache && isfile(metafile)
        try
            metadata = open(deserialize, metafile, "r")
        catch err
            # error reading metadata
            warn("Cached metadata file is corrupt. Not using cache.")
            @goto readunknown
        end
        knownmeta = metadata[sort!(files), opthash]
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

    if !isnull(chunkrefs[1].handle.offset)
        # implicit index mode - fabricate a 1-d index space 1:n
        if metadata === nothing
            o = 1
        else
            o = last(metadata.data.columns.metadata[end].domain)[1] + 1
        end
        for c in chunkrefs
            c.handle.offset = o
            n = get(nrows(domain(c)))
            c.domain = IndexSpace(Interval((o,), (o+n-1,)),
                                  Interval((o,), (o+n-1,)), Nullable{Int}(n))
            o += n
        end
    end

    # store this back in cache
    cache = Table(Columns(unknown, fill(opthash, length(unknown)), names=[:filename, :opthash]),
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
const _read_cache = Dict{String,Any}()

type CSVChunk
    filename::String
    cache::Bool
    delim::Char
    cached_on::Vector{Int}
    opts::Dict
    offset::Nullable{Int}  # index of first item when using implicit indices
end

Dagger.affinity(c::CSVChunk) = map(OSProc, c.cached_on)

function gather(ctx, csv::CSVChunk)
    if csv.cache && haskey(_read_cache, csv.filename)
        #println("Having to fetch data from $csv.cached_on")
        data = _read_cache[csv.filename]
    elseif csv.cached_on != [myid()] && !isempty(csv.cached_on)
        # TODO: remove myid() if it's in cached_on
        pid = first(csv.cached_on)
        if pid == myid()
            pid = last(csv.cached_on)
        end
        data = remotecall_fetch(c -> gather(ctx, c), pid, csv)
    else
        #println("CACHE MISS $csv")
        data, ii = loadTable(csv.filename, csv.delim; csv.opts...)

        if ii && isnull(csv.offset)
            csv.offset = 1
        end

        if !(myid() in csv.cached_on)
            push!(csv.cached_on, myid())
        end
        _read_cache[csv.filename] = data
    end

    if !isnull(csv.offset) && data.index[1][1] != get(csv.offset)
        o = get(csv.offset)
        data.index.columns[1][:] = o:(o+length(data)-1)
    end

    return data
end

function makecsvchunk(file, delim; cache=true, opts...)
    handle = CSVChunk(file, cache, delim, Int[], Dict(opts), nothing)
    # We need to actually load the data to get things like
    # the type and Domain. It will get cached if cache is true
    nds = gather(Context(), handle)
    Dagger.Chunk(typeof(nds), domain(nds), handle, false)
end
