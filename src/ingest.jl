export ingest, ingest!, load, save

const JULIADB_INDEXFILE = "juliadb_index.jls"

"""
    ingest(files::Union{AbstractVector,String}, outputdir::AbstractString; <options>...)

ingests data from CSV files into JuliaDB. Stores the metadata and index
in a directory `outputdir`. Creates `outputdir` if it doesn't exist.

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
- `tomemory::Bool`: Load data to memory after ingesting instead of mmapping. Defaults to false.
- The rest of the keyword arguments will be passed on to [`TextParse.csvread`](@ref) which is used by this function to load data from individual files.

See also [`loadfiles`](@ref) and [`save`](@ref)
"""
function ingest(files::Union{AbstractVector,String}, outputdir::AbstractString;
                delim = ',', tomemory=false, opts...)
    dtable_file = joinpath(outputdir, JULIADB_INDEXFILE)
    if isfile(dtable_file)
        error("data already exists in $outputdir, use `ingest!` to append new files. Aborting.")
    end
    if !isdir(outputdir)
        mkdir(outputdir)
    end
    ingest!(files, outputdir; delim = delim, tomemory=tomemory, opts...)
end

"""
    ingest!(files::Union{AbstractVector,String}, outputdir::AbstractString; <options>...)

ingest data from `files` and append into data stored in `outputdir`. Creates `outputdir`
if it doesn't exist. Arguments are the same as those to [ingest](@ref). The index range of
data in the new files should not overlap with files previously ingested.

See also [`ingest`](@ingest)
"""
function ingest!(files::Union{AbstractVector,String}, outputdir::AbstractString; delim = ',', tomemory=false, opts...)
    outputdir = abspath(outputdir)

    prev_chunks = []
    dtable_file = joinpath(outputdir, JULIADB_INDEXFILE)
    existing_dtable = nothing

    if !isdir(outputdir)
        warn("$outputdir doesn't exist. Creating it")
        mkdir(outputdir)
    elseif isfile(dtable_file)
        existing_dtable = load(outputdir)
        prev_chunks = existing_dtable.chunks
    end

    if isa(files, String)
        if !isdir(files)
            throw(ArgumentError("Specified source path does not refer to an existing directory."))
        end
        files = files_from_dir(files)
    else
        for file in files
            if !isfile(file)
                throw(ArgumentError("No file named $file."))
            end
        end
        files = map(abspath, files)
    end

    if isempty(files)
        throw(ArgumentError("Specify at least one file to ingest."))
    end

    # exclude files we've already seen
    filter!(f->!any(ch->splitext(ch.handle.filename)[1] == joinpath(outputdir, normalize_filepath(f)),
                    prev_chunks),
            files)

    if isempty(files)
        assert(existing_dtable !== nothing)
        return existing_dtable
    end

    sz = sum(map(filesize, files))
    println("Reading $(length(files)) csv files totalling $(format_bytes(sz))...")

    function load_and_save(file)
        data, ii = _load_table(file, delim; opts...)
        save_as_chunk(data, joinpath(outputdir, normalize_filepath(file)), implicit_index=ii)
    end

    saved = map(delayed(load_and_save), files)

    chunkrefs = collect(get_context(), delayed(vcat)(saved...))

    if !isnull(chunkrefs[1].handle.offset)
        offset = existing_dtable===nothing ? 1 :
            reduce(max, 0, first.(last.(existing_dtable.subdomains))) + 1

        distribute_implicit_index_space!(chunkrefs, offset)
    end

    allchunks = vcat(prev_chunks, chunkrefs)
    dtable = cache_thunks(fromchunks(allchunks))

    if any(c->!isa(c, Dagger.Chunk) || !isa(c.handle, OnDisk),
           dtable.chunks)
        # This means `fromchunks` had to re-sort overlapping chunks
        tmpname = tempname()
        dtable = save(dtable, tmpname) # write out the sorted version
        # overwrite initially written data with sorted version
        mv(tmpname, outputdir; remove_destination=true)
    end

    open(io -> serialize(io, dtable), joinpath(outputdir, JULIADB_INDEXFILE), "w")
    if tomemory
        compute(mapchunks(identity, dtable))
    else
        dtable
    end
end

function normalize_filepath(filepath)
    x = replace(filepath, "/", "_")
    x = replace(x, "\\", "_")
    replace(x, ".", "_")
end

"""
    load(dir::AbstractString; tomemory)

Load a saved `DTable` from `dir` directory. Data can be saved
using `ingest` or `save` functions. If `tomemory` option is true,
then data is loaded into memory rather than mmapped.

See also [`ingest`](@ref), [`save`](@ref)
"""
function load(dir::AbstractString; tomemory=false)
    dtable_file = joinpath(dir, JULIADB_INDEXFILE)
    dtable = open(deserialize, dtable_file)
    for c in dtable.chunks
        if isa(c.handle, OnDisk)
            # clear out any affinity
            empty!(c.handle.cached_on)
        end
    end
    if tomemory
        compute(mapchunks(copy, dtable; keeplengths=true))
    else
        dtable
    end
end

"""
    save(t::DTable, outputdir::AbstractString)

Saves a `DTable` to disk. This function blocks till all
chunks have been computed and saved. Saved data can
be loaded with `load`.

See also [`ingest`](@ref), [`load`](@ref)
"""
function save(t::DTable{K,V}, outputdir::AbstractString) where {K,V}

    if !isdir(outputdir)
        mkdir(outputdir)
    end

    chunks = Any[begin
        fn = joinpath(outputdir, lpad(idx, 5, "0"))
        delayed(save_as_chunk; get_result=true)(chunk, fn)
    end for (idx, chunk) in enumerate(t.chunks)]
    
    saved_t = DTable{K,V}(t.subdomains, chunks)

    final = compute(saved_t)
    cached_on = map(c->copy(c.handle.cached_on), final.chunks)
    for c in final.chunks
        if isa(c.handle, OnDisk)
            # clear out any affinity
            c.handle.cached_on = []
        end
    end
    open(io -> serialize(io, final), joinpath(outputdir, JULIADB_INDEXFILE), "w")
    for (c, pids) in zip(final.chunks, cached_on)
        if isa(c.handle, OnDisk)
            # restore old affinity
            c.handle.cached_on = pids
        end
    end
    final
end

mutable struct OnDisk
    filename::String
    cached_on::Vector{Int}
    cache::Bool
    offset::Nullable{Int}  # index of first item when using implicit indices
end

function Dagger.affinity(c::OnDisk)
    sz = filesize(c.filename)
    map(c.cached_on) do p
        OSProc(p) => sz
    end
end

## TODO: Can make this an LRU cache
const _ondisk_cache = Dict{String, Any}()

function collect(ctx::Context, d::OnDisk)
    if d.cache && haskey(_ondisk_cache, d.filename)
        data = _ondisk_cache[d.filename]
    else
        data = unwrap_mmap(open(deserialize, d.filename))
        if !(myid() in d.cached_on)
            push!(d.cached_on, myid())
        end
        _ondisk_cache[d.filename] = data
    end

    if !isnull(d.offset) && data.index[1][1] != get(d.offset)
        o = get(d.offset)
        data.index.columns[1][:] = o:(o+length(data)-1)
    end

    return data
end

function save_as_chunk(data, filename_base; cache=true, implicit_index=false)
    jlsfile = filename_base * ".jls"
    mmapfile = filename_base * ".mmap"
    mmapped_data = save_table(data, jlsfile, mmapfile)
    _ondisk_cache[jlsfile] = unwrap_mmap(mmapped_data)
    Dagger.Chunk(typeof(data), domain(data),
                 OnDisk(jlsfile, Int[myid()], cache, implicit_index ? 1 : nothing),
                 false)
end

function save_table(data::Table, file, mmap_file = file * ".mmap")
    ondiskdata = copy_mmap(mmap_file, data)
    open(io -> serialize(io, ondiskdata), file, "w")
    ondiskdata
end
