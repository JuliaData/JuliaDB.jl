export ingest, ingest!, load, save

const JULIADB_INDEXFILE = "juliadb_index.jls"

"""
    ingest(files::Union{AbstractVector,String}, outputdir::AbstractString; <options>...)

ingests data from CSV files into JuliaDB. Stores the metadata and index
in a directory `outputdir`. Creates `outputdir` if it doesn't exist.

# Arguments:

- `delim`: the delimiter to use to read the text file with data. defaults to `,`
- `indexcols`: columns that are meant to act as the index for the table.
   Defaults to all but the last column. If `datacols` is set, defaults to all
   columns other than the data columns.
- `indexcols`: columns that are meant to act as the data for the table.
   Defaults to the last column. If `indexcols` is set, defaults to all
   columns other than the index columns.
- `agg`: aggregation function to use to combine data points with the same index.
    Defaults to nothing. (Currently `agg` only works on each chunk and not the whole
    table.)
- `presorted`: whether the data in each chunk is pre-sorted.
- `copy`: whether to copy the data before presorting or aggregating
- All other options are passed on to `TextParse.csvread`
"""
function ingest(files::Union{AbstractVector,String}, outputdir::AbstractString; delim = ',', opts...)
    dtable_file = joinpath(outputdir, JULIADB_INDEXFILE)
    if isfile(dtable_file)
        error("data already exists in $outputdir, use `ingest!` to append new files. Aborting.")
    end
    if !isdir(outputdir)
        mkdir(outputdir)
    end
    ingest!(files, outputdir; delim = delim, opts...)
end

"""
    ingest!(files::Union{AbstractVector,String}, outputdir::AbstractString; <options>...)

ingest data from `files` and append into data stored in `outputdir`. Creates `outputdir`
if it doesn't exist. Arguments are the same as those to [ingest](@ref). The index range of
data in the new files should not overlap with files previously ingested.
"""
function ingest!(files::Union{AbstractVector,String}, outputdir::AbstractString; delim = ',', opts...)
    outputdir = abspath(outputdir)

    prev_chunks = []
    dtable_file = joinpath(outputdir, JULIADB_INDEXFILE)
    existing_dtable = nothing

    if !isdir(outputdir)
        warn("$outputdir doesn't exist. Creating it")
        mkdir(outputdir)
    elseif isfile(dtable_file)
        existing_dtable = load(outputdir)
        prev_chunks = chunks(existing_dtable).data.columns.chunk
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
        data, _ = loadTable(file, delim; opts...)
        save_as_chunk(data, joinpath(outputdir, normalize_filepath(file)))
    end

    saved = map(delayed(load_and_save), files)

    allchunks = vcat(prev_chunks, gather(delayed(vcat)(saved...)))
    filenames1 = map(c -> c.handle.filename, allchunks)
    dtable = fromchunks(allchunks)

    if any(c->!isa(c.handle, OnDisk), dtable.chunks.data.columns.chunk)
        # This means `fromchunks` had to re-sort overlapping chunks
        dtable = save(dtable, outputdir) # write out the sorted version
        filenames2 = map(c -> c.handle.filename, dtable.chunks.data.columns.chunk)
        Base.foreach(rm, setdiff(filenames1, filenames2))
    end

    open(io -> serialize(io, dtable), joinpath(outputdir, JULIADB_INDEXFILE), "w")
    dtable
end

function normalize_filepath(filepath)
    x = replace(filepath, "/", "_")
    x = replace(x, "\\", "_")
    replace(x, ".", "_")
end

"""
    load(dir::AbstractString)

Load a saved `DTable` from `dir` directory. Data can be saved
using `ingest` or `save` functions.
"""
function load(dir::AbstractString)
    dtable_file = joinpath(dir, JULIADB_INDEXFILE)
    open(deserialize, dtable_file)
end

"""
    save(t::DTable, outputdir::AbstractString)

Saves a `DTable` to disk. This function blocks till all
files data has been computed and saved. Saved data can
be loaded with `load`.
"""
function save(t::DTable, outputdir::AbstractString)

    if !isdir(outputdir)
        mkdir(outputdir)
    end

    saved_t = withchunksindex(t) do c
        datacols = c.data.columns
        chunkscol = Any[begin
            fn = joinpath(outputdir, lpad(idx, 5, "0"))
            delayed(save_as_chunk; get_result=true)(chunk, fn)
        end for (idx, chunk) in enumerate(datacols.chunk)]
        Table(c.index,
                 Columns(datacols.boundingrect,
                         chunkscol,
                         datacols.length,
                         names=[:boundingrect, :chunk, :length]))
    end

    final = compute(saved_t)
    open(io -> serialize(io, final), joinpath(outputdir, JULIADB_INDEXFILE), "w")
    final
end

type OnDisk
    filename::String
    cached_on::Vector{Int}
    cache::Bool
end

Dagger.affinity(c::OnDisk) = map(OSProc, c.cached_on)

## TODO: Can make this an LRU cache
const _ondisk_cache = Dict{String, Any}()

function gather(ctx, d::OnDisk)
    if d.cache && haskey(_ondisk_cache, d.filename)
        _ondisk_cache[d.filename]
    else
        data = unwrap_mmap(open(deserialize, d.filename))
        if !(myid() in d.cached_on)
            push!(d.cached_on, myid())
        end
        _ondisk_cache[d.filename] = data
    end
end

function save_as_chunk(data, filename_base; cache=true)
    jlsfile = filename_base * ".jls"
    mmapfile = filename_base * ".mmap"
    mmapped_data = save_table(data, jlsfile, mmapfile)
    _ondisk_cache[jlsfile] = unwrap_mmap(mmapped_data)
    Dagger.Chunk(typeof(data), domain(data), OnDisk(jlsfile, Int[myid()], cache), false)
end

function save_table(data::Table, file, mmap_file = file * ".mmap")
    ondiskdata = copy_mmap(mmap_file, data)
    open(io -> serialize(io, ondiskdata), file, "w")
    ondiskdata
end
