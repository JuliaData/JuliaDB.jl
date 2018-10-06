export loadfiles, ingest, ingest!, load, save, loadndsparse, loadtable
import Base: serialize, deserialize
import Dagger: refcount_chunks

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

function offset_index!(x, o)
    l = length(x)
    copy!(columns(x.index)[1], o:o+l-1)
    x
end

function offset_index!(x::DNDSparse, o=1)
    lengths = map(a->get(a.nrows), x.domains)
    offs = [0, cumsum(lengths[1:end-1]);] .+ 1
    fromchunks(delayedmap(offset_index!, x.chunks, offs))
end

Base.@deprecate loadfiles(files, delim=','; opts...) loadndsparse(files; delim=delim, opts...)

"""
`loadtable(files::Union{AbstractVector,String}; <options>)`

Load a [table](@ref Table) from CSV files.

`files` is either a vector of file paths, or a directory name.

# Options:

- `output::AbstractString` -- directory name to write the table to. By default data is loaded directly to memory. Specifying this option will allow you to load data larger than the available memory.
- `indexcols::Vector` -- columns to use as primary key columns. (defaults to [])
- `datacols::Vector` -- non-indexed columns. (defaults to all columns but indexed columns). Specify this to only load a subset of columns. In place of the name of a column, you can specify a tuple of names -- this will treat any column with one of those names as the same column, but use the first name in the tuple. This is useful when the same column changes name between CSV files. (e.g. `vendor_id` and `VendorId`)
- `distributed::Bool` -- should the output dataset be loaded as a distributed table? If true, this will use all available worker processes to load the data. (defaults to true if workers are available, false if not)
- `chunks::Int` -- number of chunks to create when loading distributed. (defaults to number of workers)
- `delim::Char` -- the delimiter character. (defaults to `,`). Use `spacedelim=true` to split by spaces.
- `spacedelim::Bool`: parse space-delimited files. `delim` has no effect if true.
- `quotechar::Char` -- quote character. (defaults to `"`)
- `escapechar::Char` -- escape character. (defaults to `"`)
- `filenamecol::Union{Symbol, Pair}` -- create a column containing the file names from where each row came from. This argument gives a name to the column. By default, `basename(name)` of the name is kept, and ".csv" suffix will be stripped. To provide a custom function to apply on the names, use a `name => Function` pair. By default, no file name column will be created.
- `header_exists::Bool` -- does header exist in the files? (defaults to true)
- `colnames::Vector{String}` -- specify column names for the files, use this with (`header_exists=false`, otherwise first row is discarded). By default column names are assumed to be present in the file.
- `samecols` -- a vector of tuples of strings where each tuple contains alternative names for the same column. For example, if some files have the name "vendor_id" and others have the name "VendorID", pass `samecols=[("VendorID", "vendor_id")]`.
- `colparsers` -- either a vector or dictionary of data types or an [`AbstractToken` object](https://juliacomputing.com/TextParse.jl/stable/#Available-AbstractToken-types-1) from [TextParse](https://juliacomputing.com/TextParse.jl/stable) package. By default, these are inferred automatically. See `type_detect_rows` option below.
- `type_detect_rows`: number of rows to use to infer the initial `colparsers` defaults to 20.
- `nastrings::Vector{String}` -- strings that are to be considered NA. (defaults to `TextParse.NA_STRINGS`)
- `skiplines_begin::Char` -- skip some lines in the beginning of each file. (doesn't skip by default)

- `usecache::Bool`: (vestigial)
"""
function loadtable(files::Union{AbstractVector,String}; opts...)
    _loadtable(NextTable, files; opts...)
end

"""
`loadndsparse(files::Union{AbstractVector,String}; <options>)`

Load an [NDSparse](@ref) from CSV files.

`files` is either a vector of file paths, or a directory name.

# Options:

- `indexcols::Vector` -- columns to use as indexed columns. (by default a `1:n` implicit index is used.)
- `datacols::Vector` -- non-indexed columns. (defaults to all columns but indexed columns). Specify this to only load a subset of columns. In place of the name of a column, you can specify a tuple of names -- this will treat any column with one of those names as the same column, but use the first name in the tuple. This is useful when the same column changes name between CSV files. (e.g. `vendor_id` and `VendorId`)

All other options are identical to those in [`loadtable`](@ref)

"""
function loadndsparse(files::Union{AbstractVector,String}; opts...)
    _loadtable(NDSparse, files; opts...)
end

# Can load both NDSparse and table
function _loadtable(T, files::Union{AbstractVector,String};
                    chunks=nothing,
                    output=nothing,
                    append=false,
                    indexcols=[],
                    distributed=chunks != nothing || length(procs()) > 1,
                    usecache=false,
                    opts...)

    if isa(files, String)
        if isdir(files)
            files = files_from_dir(files)
        elseif isfile(files)
            files = [files]
        else
            throw(ArgumentError("Specified path is neither a file, " *
                                "nor a directory."))
        end
    else
        for file in files
            if !isfile(file)
                throw(ArgumentError("No file named $file."))
            end
        end
    end

    if isempty(files)
        throw(ArgumentError("Specify at least one file to load."))
    end

    if chunks === nothing && distributed
        chunks = nworkers()
    end

    if !distributed
        filegroups = [files]
    else
        if isa(chunks, Integer)
            chunks = Dagger.split_range(1:length(files), chunks)
        end

        filegroups = filter(!isempty, map(x->files[x], chunks))
    end

    loadgroup = delayed() do group
        _loadtable_serial(T, group; indexcols=indexcols, opts...)[1]
    end

    if output !== nothing && append
        prevchunks = load(output).chunks
    else
        prevchunks = []
    end

    y = fromchunks(map(loadgroup, filegroups),
                   output=output, fnoffset=length(prevchunks))
    x = fromchunks(vcat(prevchunks, y.chunks))

    if output !== nothing
        open(joinpath(output, JULIADB_INDEXFILE), "w") do io
            serialize(io, x)
        end
        _makerelative!(x, output)
    end

    if x isa DNDSparse && isempty(indexcols)
        # implicit index
        x = offset_index!(x, 1)
    end

    if !distributed
        return collect(x)
    else
        return x
    end
end

Base.@deprecate ingest(files, output; kwargs...) loadndsparse(files; output=output, kwargs...)

Base.@deprecate ingest!(files, output; kwargs...) loadndsparse(files; output=output, append=true, kwargs...)

"""
`load(dir::AbstractString; tomemory)`

Load a saved `DNDSparse` from `dir` directory. Data can be saved
using the `save` function.
"""
function load(f::AbstractString)
    if isdir(f)
        x = open(joinpath(f, JULIADB_INDEXFILE)) do io
            deserialize(io)
        end
        _makerelative!(x, f)
        x
    elseif isfile(f)
        MemPool.unwrap_payload(open(deserialize, f))
    else
        error("$f is not a file or directory")
    end
end

"""
`save(t::DNDSparse, outputdir::AbstractString)`

Saves a distributed dataset to disk. Saved data can be loaded with `load`.
"""
function save(x::DDataset, output::AbstractString)
    if !isempty(x.chunks)
        y = fromchunks(x.chunks, output=output)
    else
        y = x
    end
    open(joinpath(output, JULIADB_INDEXFILE), "w") do io
        serialize(io, y)
    end
    _makerelative!(y, output)
    y
end

function save(data::Dataset, f::AbstractString)
    sz = open(f, "w") do io
        serialize(io, MemPool.MMWrap(data))
    end
    load(f)
end

function _makerelative!(t, dir::AbstractString)
    foreach(t.chunks) do c
        h = c.handle
        if isa(h, FileRef)
            c.handle = FileRef(joinpath(dir, h.file), h.size)
        end
    end
end

function serialize(io::AbstractSerializer, A::Union{DNextTable,DNDSparse})
    @async refcount_chunks(A)
    invoke(serialize, Tuple{AbstractSerializer,Any}, io, A)
end

deserialize(io::AbstractSerializer, DT::Type{DNDSparse{K,V}}) where {K,V} = _deser(io, DT)
deserialize(io::AbstractSerializer, DT::Type{DNextTable{T,K}}) where {T,K} = _deser(io, DT)
function _deser(io::AbstractSerializer, t)
    nf = nfields(t)
    x = ccall(:jl_new_struct_uninit, Any, (Any,), t)
    t.mutable && Base.Serializer.deserialize_cycle(io, x)
    for i in 1:nf
        tag = Int32(read(io.io, UInt8)::UInt8)
        if tag != Base.Serializer.UNDEFREF_TAG
            ccall(:jl_set_nth_field, Void, (Any, Csize_t, Any), x, i-1, Base.Serializer.handle_deserialize(io, tag))
        end
    end
    finalizer(x, free!)
    return x
end

refcount_chunks(A::Union{DNextTable,DNDSparse}) = refcount_chunks(A.chunks)

using WeakRefStrings

function MemPool.mmwrite(io::AbstractSerializer, arr::StringArray)
    Base.serialize_type(io, MemPool.MMSer{StringArray})
    serialize(io, eltype(arr))
    MemPool.mmwrite(io, arr.buffer)
    MemPool.mmwrite(io, arr.offsets)
    MemPool.mmwrite(io, arr.lengths)
    return
end

function MemPool.mmread(::Type{StringArray}, io, mmap)
    T = deserialize(io)
    buffer  = deserialize(io)
    offsets = deserialize(io)
    lengths  = deserialize(io)
    return StringArray{T, ndims(offsets)}(buffer, offsets, lengths)
end
