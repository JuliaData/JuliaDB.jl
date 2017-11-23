export loadfiles, ingest, ingest!, load, save, loadndsparse, loadtable

const JULIADB_DIR = ".juliadb"
const JULIADB_FILECACHE = "csv_metadata"
const JULIADB_INDEXFILE = "juliadb_index"

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

Base.@deprecate loadfiles(files, delim=','; opts...) loadndsparse(files; delim=delim, opts...)

"""
`loadtable(files::Union{AbstractVector,String}; <options>)`

Load a [table](@ref Table) from CSV files.

`files` is either a vector of file paths, or a directory name.

# Options:

- `indexcols::Vector` -- columns to use as primary key columns. (defaults to [])
- `datacols::Vector` -- non-indexed columns. (defaults to all columns but indexed columns)
- `distributed::Bool` -- should the output dataset be loaded in a distributed way? If true, this will use all available worker processes to load the data. (defaults to true if workers are available, false if not)
- `chunks::Bool` -- number of chunks to create when loading distributed. (defaults to number of workers)
- `delim::Char` -- the delimiter character. (defaults to `,`)
- `quotechar::Char` -- quote character. (defaults to `"`)
- `escapechar::Char` -- escape character. (defaults to `\\`)
- `header_exists::Bool` -- does header exist in the files? (defaults to true)
- `colnames::Vector{String}` -- specify column names for the files, use this with (`header_exists=true`, otherwise first row is discarded). By default column names are assumed to be present in the file.
- `samecols` -- a vector of tuples of strings where each tuple contains alternative names for the same column. For example, if some files have the name "vendor_id" and others have the name "VendorID", pass `samecols=[("VendorID", "vendor_id")]`.
- `colparsers` -- either a vector or dictionary of data types or an [`AbstractToken` object](https://juliacomputing.com/TextParse.jl/stable/#Available-AbstractToken-types-1) from [TextParse](https://juliacomputing.com/TextParse.jl/stable) package. By default, these are inferred automatically. See `type_detect_rows` option below.
- `type_detect_rows`: number of rows to use to infer the initial `colparsers` defaults to 20.
- `nastrings::Vector{String}` -- strings that are to be considered NA. (defaults to `TextParse.NA_STRINGS`)
- `skiplines_begin::Char` -- skip some lines in the beginning of each file. (doesn't skip by default)

- `usecache::Bool`: use cached metadata from previous loads while loading the files. Set this to `false` if you are changing other options.
"""
function loadtable(files::Union{AbstractVector,String}; opts...)
    _loadtable(NextTable, files; opts...)[1]
end

"""
`loadndsparse(files::Union{AbstractVector,String}; <options>)`

Load an [NDSparse](@ref) from CSV files.

`files` is either a vector of file paths, or a directory name.

# Options:

- `indexcols::Vector` -- columns to use as indexed columns. (by default a `1:n` implicit index is used.)
- `datacols::Vector` -- non-indexed columns. (defaults to all columns but indexed columns)
All other options are identical to those in [`loadtable`](@ref)

"""
function loadndsparse(files::Union{AbstractVector,String}; opts...)
    _loadtable(NDSparse, files; opts...)[1]
end

# Can load both NDSparse and table
function _loadtable(T, files::Union{AbstractVector,String};
                    chunks=nothing,
                    distributed=chunks != nothing || length(procs()) > 1,
                    delim=',', usecache=true, opts...)

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

    if chunks === nothing && distributed
        chunks = nworkers()
    end

    if !distributed
        filegroups = [files]
    else
        if isa(chunks, Int)
            chunks = Dagger.split_range(1:length(files), chunks)
        end

        filegroups = filter(!isempty, map(x->files[x], chunks))
    end

    unknown = filegroups
    validcache = []
    metadata = nothing

    # Read metadata about a subset of files if safe to
    ext = T <: NextTable ? ".tbl" : ".nds"
    metafile = joinpath(cachedir, JULIADB_FILECACHE * ext)
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
        ii = !isnull(validcache[1].handle.offset)
        if !distributed
            return collect(validcache[1]), ii
        end
        return cache_thunks(fromchunks(validcache)), ii
    end

    allfiles = collect(Iterators.flatten(unknown))
    sz = sum(filesize, allfiles)
    batches = chunks !== nothing ? length(chunks) : 1
    println("Reading $(length(allfiles)) csv files totalling $(format_bytes(sz)) in $(batches) batches...")
    # Load the data first into memory
    load_f(f) = makecsvchunk(T, f, delim; opts...)
    data = map(delayed(load_f), unknown)

    chunkrefs = collect(get_context(), delayed((xs...)->[xs...])(data...))

    ii = !isnull(chunkrefs[1].handle.offset)
    if T<:NDSparse && ii
        lastidx = reduce(max, 0, first.(last.(domain.(validcache)))) + 1
        distribute_implicit_index_space!(chunkrefs, lastidx)
    end

    # store this back in cache
    cache = Columns(unknown, map(g->mtime.(g), unknown),
            convert(Array{Dagger.Chunk}, chunkrefs),
            names=[:files, :mtime, :metadata])

    if metadata != nothing
        cache = vcat(metadata, cache)
    end

    order = [findfirst(column(cache, :files), f) for f in filegroups] # keep order of the input files
    cs = column(cache, :metadata)[order]

    if T<:NDSparse && !isnull(chunkrefs[1].handle.offset)
        distribute_implicit_index_space!(cs, 1)
    end
    ii = ii || !isnull(chunkrefs[1].handle.offset)

    open(metafile, "w") do io
        serialize(io, cache)
    end

    if !distributed
        return collect(cs[1]), ii
    else
        return cache_thunks(fromchunks(cs)), ii
    end
end

## CSV reader
const _read_cache = Dict{Tuple{Type, Vector{String}, Dict},Any}()
const _cached_on = Dict{Tuple{Type, Vector{String}, Dict},Any}() # workers register here once they have read a file

mutable struct CSVChunk
    T::Type
    files::AbstractArray
    cache::Bool
    delim::Char
    opts::Dict
    offset::Nullable{Int}  # index of first item when using implicit indices
end

# make sure cache matches a certain subset of options
csvkey(csv::CSVChunk) = (csv.T, csv.files, filter((k,v)->(k in (:colnames,:indexcols,:datacols)), csv.opts))

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
        ii = false
        data, ii = _loadtable_serial(csv.T, csv.files; delim=csv.delim, csv.opts...)

        if ii && isnull(csv.offset)
            csv.offset = 1
        end

        _read_cache[key] = data, ii
        mypid = myid()
        remotecall(1) do # tell master you have it
            push!(Base.@get!(JuliaDB._cached_on, key, []), mypid)
        end
    end

    if isa(data, NDSparse) && ii && data.index[1][1] != get(csv.offset, 1)
        o = get(csv.offset,1)
        data.index.columns[1][:] = o:(o+length(data)-1)
    end

    return data, ii
end

function makecsvchunk(T, file, delim; cache=true, opts...)
    handle = CSVChunk(T, file, cache, delim, Dict(opts), nothing)
    # We need to actually load the data to get things like
    # the type and Domain. It will get cached if cache is true
    nds, ii = _collect(get_context(), handle)
    if ii
        handle.offset = 1
    end
    Dagger.Chunk(typeof(nds), domain(nds), handle, false)
end


"""
    ingest(files::Union{AbstractVector,String}, outputdir::AbstractString; <options>...)

ingests data from CSV files into JuliaDB. Stores the metadata and index
in a directory `outputdir`. Creates `outputdir` if it doesn't exist.

All keyword arguments are passed to loadfiles. `delim` is a keyword argument to `ingest` -- this is the delimiter in CSV reading.

Equivalent to calling `loadfiles` and then `save` on the result of
`loadfiles`. See also [`loadfiles`](@ref) and [`save`](@ref)
"""
function ingest(files::Union{AbstractVector,String}, outputdir::AbstractString; opts...)
    save(loadndsparse(files; opts...), outputdir)
end

"""
    ingest!(files::Union{AbstractVector,String}, inputdir::AbstractString, outpudir=inputdir; delim=',', chunks=1, <options>...)

load data from `files` and add it to data stored in `inputdir`. If `outputdir` is specified, the resulting table will be written to this directory. By default, this will create a single chunk and append it. Pass `chunks` argument to specify how many chunks the new data should be loaded as.

See also [`ingest`](@ingest)
"""
function ingest!(files::Union{AbstractVector,String}, inputdir::AbstractString, outputdir=nothing; delim = ',', chunks=1, opts...)
    if outputdir === nothing
        outputdir = inputdir
    end

    x = load(outputdir)
    y, ii = _loadtable(NDSparse, files; delim=delim, chunks=chunks, opts...)
    if ii && isa(y, DNDSparse)
        # append new chunk by setting the implicit index
        len = get(trylength(x))
        distribute_implicit_index_space!(y.chunks, len+1)
        m = fromchunks(vcat(x.chunks, y.chunks))
    else
        if isa(y, DNDSparse)
            y = distribute(y, 1)
        end
        m = merge(x, y, agg=nothing) # this will keep repeated vals
    end
    if abspath(outputdir) == abspath(inputdir)
        # save in a temporary file, delete original
        # move temp to original
        t = tempname()
        save(m, t)
        rm(outputdir; recursive=true)
        mv(t, outputdir)
        load(outputdir)
    else
        save(m, outputdir)
    end
end


"""
`load(dir::AbstractString; tomemory)`

Load a saved `DNDSparse` from `dir` directory. Data can be saved
using the `save` function.
"""
function load(dir::AbstractString; copy=false)
    dtable_file = joinpath(dir, JULIADB_INDEXFILE)
    t = open(deserialize, dtable_file)
    _makerelative!(t, dir)
    t
end

"""
`save(t::Union{DNDSparse, DTable}, outputdir::AbstractString)`

Saves a distributed dataset to disk. Saved data can be loaded with `load`.
"""
function save(t::DDataset, outputdir::AbstractString)
    chunks = Dagger.savechunks(t.chunks, outputdir)
    saved_t = fromchunks(chunks)
    open(joinpath(outputdir, JULIADB_INDEXFILE), "w") do io
        serialize(io, saved_t)
    end
    _makerelative!(saved_t, outputdir)
    saved_t
end

function _makerelative!(t, dir::AbstractString)
    foreach(t.chunks) do c
        h = c.handle
        if isa(h, FileRef)
            c.handle = FileRef(joinpath(dir, h.file), h.size)
        end
    end
end
