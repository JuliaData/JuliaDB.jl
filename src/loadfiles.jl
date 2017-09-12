export loadfiles

const JULIADB_DIR = ".juliadb"
const JULIADB_FILECACHE = "filemeta.dat"

function files_from_dir(dir)
    dir = abspath(dir)
    filter(isfile, [ joinpath(dir, f) for f in readdir(dir) if !startswith(f, ".") ])
end

function format_bytes(nb)
    bytes, mb = Base.prettyprint_getunits(nb, length(Base._mem_units), Int64(1024))
    if mb == 1
        @sprintf("%d %s%s", bytes, Base._mem_units[mb], bytes==1 ? "" : "s")
    else
        @sprintf("%.3f %s", bytes, Base._mem_units[mb])
    end
end

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

- `usecache::Bool`: use cached metadata from previous loads while loading the files. Set this to `false` if you are changing other options.

All other arguments options are the same as those listed in [`ingest`](@ref).

See also [`ingest`](@ref).
"""
function loadfiles(files::Union{AbstractVector,String}, delim=','; usecache=true, chunks=nworkers(), opts...)

    if isa(files, String)
        if !isdir(files)
            throw(ArgumentError("Specified path does not refer to an existing directory."))
        end
        cachedir = joinpath(files, JULIADB_DIR)
        files = files_from_dir(files)
    else
        for file in files
            if !isfile(file)
                throw(ArgumentError("No file named $file."))
            end
        end
        cachedir = JULIADB_DIR
    end

    if isempty(files)
        throw(ArgumentError("Specify at least one file to load."))
    end

    if !isdir(cachedir)
        mkdir(cachedir)
    end

    if isa(chunks, Int)
        chunks = Dagger.split_range(1:length(files), chunks)
    end

    filegroups = map(x->files[x], chunks)

    unknown = filegroups
    validcache = []
    metadata = nothing

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
        knownidx = find(row -> row.files in filegroups, metadata)
        knownmeta = metadata[knownidx]
        known = column(metadata, :files)[knownidx]

        # only those with the same mtime
        valid = column(knownmeta, :mtime) .== map(fs->mtime.(fs), known)
        validcache = column(knownmeta,:metadata)[valid]
        unknown = setdiff(filegroups, known[valid])

    end
    @label readunknown

    # Give an idea of what we're up against, we should probably also show a
    # progress meter.
    println("Metadata for ", length(files)-sum(length.(unknown)), " / ",
            length(files), " files can be loaded from cache.")

    if isempty(unknown)
        # we read all required metadata from cache
        return cache_thunks(fromchunks(validcache))
    end

    allfiles = collect(Iterators.flatten(unknown))
    sz = sum(filesize, allfiles)
    println("Reading $(length(allfiles)) csv files totalling $(format_bytes(sz)) in $(length(chunks)) batches...")
    # Load the data first into memory
    load_f(f) = makecsvchunk(f, delim; opts...)
    data = map(delayed(load_f), unknown)

    chunkrefs = collect(get_context(), delayed(vcat)(data...))

    if !isnull(chunkrefs[1].handle.offset)
        lastidx = reduce(max, 0, first.(last.(domain.(validcache)))) + 1
        distribute_implicit_index_space!(chunkrefs, lastidx)
    end

    # store this back in cache
    cache = Columns(unknown, map(g->mtime.(g), unknown),
            convert(Array{Dagger.Chunk}, chunkrefs),
            names=[:files, :mtime, :metadata])

    if metadata != nothing
        cache = merge(metadata, cache)
    end

    order = [findfirst(column(cache, :files), f) for f in filegroups] # keep order of the input files
    cs = column(cache, :metadata)[order]

    if !isnull(chunkrefs[1].handle.offset)
        distribute_implicit_index_space!(cs, 1)
    end

    open(metafile, "w") do io
        serialize(io, cache)
    end

    cache_thunks(fromchunks(cs))
end

## TODO: Can make this an LRU cache
const _read_cache = Dict{Tuple{Vector{String}, Dict},Any}()
const _cached_on = Dict{Tuple{Vector{String}, Dict},Any}() # workers register here once they have read a file

mutable struct CSVChunk
    files::AbstractArray
    cache::Bool
    delim::Char
    opts::Dict
    offset::Nullable{Int}  # index of first item when using implicit indices
end

# make sure cache matches a certain subset of options
csvkey(csv::CSVChunk) = (csv.files, filter((k,v)->(k in (:colnames,:indexcols,:datacols)), csv.opts))

function Dagger.affinity(c::CSVChunk)
    # use filesize as a measure of data size
    key = csvkey(c)
    sz = sum(filesize.(c.files))
    map(get(_cached_on, key, [])) do p
        OSProc(p) => sz
    end
end

function collect(ctx::Context, csv::CSVChunk)
    _collect(ctx, csv)[1]
end

function _collect(ctx, csv::CSVChunk)
    key = csvkey(csv)
    cached_on = remotecall_fetch(()->get(_cached_on, key, Int[]), 1)
    if csv.cache && haskey(_read_cache, key)
        data, ii = _read_cache[key]
    elseif !isempty(cached_on) && (myid() in cached_on)
        #println("Having to fetch data from $(csv.cached_on)")
        pid = first(cached_on)
        data, ii = remotecall_fetch(c -> _collect(ctx, c), pid, csv)
        _read_cache[key] = data, ii
        mypid = myid() # tell master you've got it too
        remotecall(1) do
            push!(Base.@get!(JuliaDB._cached_on, key, []), mypid)
        end
    else
        #println("CACHE MISS $csv")
        #@show myid()
        try
            data, ii = _load_table(csv.files, csv.delim; csv.opts...)
        catch err
            # show which file failed to load
            println(STDERR, "Error reading file $(csv.files)")
            rethrow(err)
        end

        if ii && isnull(csv.offset)
            csv.offset = 1
        end

        _read_cache[key] = data, ii
        mypid = myid()
        remotecall(1) do # tell master you have it
            push!(Base.@get!(JuliaDB._cached_on, key, []), mypid)
        end
    end

    if ii && data.index[1][1] != get(csv.offset, 1)
        o = get(csv.offset,1)
        data.index.columns[1][:] = o:(o+length(data)-1)
    end

    return data, ii
end

function makecsvchunk(file, delim; cache=true, opts...)
    handle = CSVChunk(file, cache, delim, Dict(opts), nothing)
    # We need to actually load the data to get things like
    # the type and Domain. It will get cached if cache is true
    nds, ii = _collect(get_context(), handle)
    if ii
        handle.offset = 1
    end
    Dagger.Chunk(typeof(nds), domain(nds), handle, false)
end
