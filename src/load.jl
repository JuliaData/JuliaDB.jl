export loadfiles

const JULIADB_CACHEDIR = ".juliadb_cache"
const JULIADB_FILECACHE = "filemeta.dat"

function files_from_dir(dir)
    dir = abspath(dir)
    filter(isfile, [ joinpath(dir, f) for f in readdir(dir) ])
end

function format_bytes(nb)
    bytes, mb = Base.prettyprint_getunits(nb, length(Base._mem_units), Int64(1024))
    if mb == 1
        @sprintf("%d %s%s", bytes, Base._mem_units[mb], bytes==1 ? "" : "s")
    else
        @sprintf("%.3f %s", bytes, Base._mem_units[mb])
    end
end

lastindex(d::DTable) = last(d.data.columns.metadata[end].domain)

# update chunk offsets and domains to form a distributed index space o:(o+n-1)
function distribute_implicit_index_space!(chunkrefs, o=1)
    for c in chunkrefs
        c.handle.offset = o
        n = get(nrows(domain(c)))
        c.domain = IndexSpace(Interval((o,), (o+n-1,)),
                              Interval((o,), (o+n-1,)), Nullable{Int}(n))
        o += n
    end
end

"""
    loadfiles(files::Union{AbstractVector,String}, delim = ','; <options>)

Load a collection of CSV `files` into a DTable, where `files` is either a vector
of file paths, or the path of a directory containing files to load.

# Arguments:

- `delim::Char`: the delimiter to use to read the text file with data. defaults to `,`
- `indexcols::AbstractArray`: columns that are meant to act as the index for the table.
   Defaults to all but the last column. If `datacols` is set, defaults to all
   columns other than the data columns. If `indexcols` is an empty vector,
   an implicit index of itegers `1:n` is added to the data.
- `datacols::AbstractArray`: columns that are meant to act as the data for the table.
   Defaults to the last column. If `indexcols` is set, defaults to all
   columns other than the index columns.
- `agg::Function`: aggregation function to use to combine data points with the same index. Defaults to nothing which leaves the data unaggregated (see [`aggregate`](@ref) to aggregate post-loading)).
   table.)
- `presorted::Bool`: specifies if each CSV file is internally already sorted according
   to the specified index column. This will avoid a re-sorting.
- `usecache::Bool`: use cached metadata from previous loads while loading the files. Set this to `false` if you are changing other options.
- The rest of the keyword arguments will be passed on to [`TextParse.csvread`](@ref) which is used by this function to load data from individual files.

See also [`ingest`](@ref).
"""
function loadfiles(files::Union{AbstractVector,String}, delim=','; usecache=true, opts...)

    if isa(files, String)
        if !isdir(files)
            throw(ArgumentError("Specified path does not refer to an existing directory."))
        end
        cachedir = joinpath(files, JULIADB_CACHEDIR)
        files = files_from_dir(files)
    else
        for file in files
            if !isfile(file)
                throw(ArgumentError("No file named $file."))
            end
        end
        cachedir = JULIADB_CACHEDIR
    end

    if isempty(files)
        throw(ArgumentError("Specify at least one file to load."))
    end

    if !isdir(cachedir)
        mkdir(cachedir)
    end

    unknown = files
    validcache = []
    metadata = nothing

    # there can be multiple Table possible in the same file
    # we hope that each one has a unique hash:
    opthash = hash(Dict(opts))
    # Read metadata about a subset of files if safe to
    metafile = joinpath(cachedir, JULIADB_FILECACHE)
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
        return fromchunks(validcache, false)
    end

    sz = sum(map(filesize, unknown))
    println("Reading $(length(unknown)) csv files totalling $(format_bytes(sz))...")
    # Load the data first into memory
    load_f(f) = makecsvchunk(f, delim; opts...)
    data = map(delayed(load_f), unknown)

    chunkrefs = gather(delayed(vcat)(data...))

    if !isnull(chunkrefs[1].handle.offset)
        distribute_implicit_index_space!(chunkrefs,
                                         metadata===nothing ? 1 : lastindex(metadata)[1] + 1)
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
const _read_cache = Dict{Tuple{String, Dict},Any}()

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
    if csv.cache && haskey(_read_cache, (csv.filename, csv.opts))
        #println("Having to fetch data from $csv.cached_on")
        data = _read_cache[(csv.filename, csv.opts)]
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
        _read_cache[(csv.filename, csv.opts)] = data
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
