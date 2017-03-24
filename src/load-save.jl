export ingest, load, save

const JULIADB_INDEXFILE = "juliadb_index.jls"

"""
    ingest(files::AbstractVector, outputdir::AbstractString; <options>...)

ingests data from CSV files into JuliaDB. Stores the metadata and index
in a directory `outputdir`.

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
function ingest(files::AbstractVector, outputdir::AbstractString; delim = ',', opts...)

    if isempty(files)
        throw(ArgumentError("Specify at least one file to ingest."))
    end

    for file in files
        if !isfile(file)
            throw(ArgumentError("No file named $file"))
        end
    end

    if !isdir(outputdir)
        mkdir(outputdir)
    end

    found = []
    cached_chunks = []

    sz = sum(map(filesize, files))
    println("Reading $(length(files)) csv files totalling $(round(sz/2^20)) MB...")

    function load_and_save(file)
        data = loadTable(file, delim; opts...)
        save_as_chunk(data, joinpath(outputdir, normalize_filepath(file)))
    end

    saved = map(delayed(load_and_save), files)

    chunks = gather(delayed(vcat)(saved...))
    dtable = fromchunks(chunks)
    open(io -> serialize(io, dtable), joinpath(outputdir, JULIADB_INDEXFILE), "w")
    return dtable
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
be loaded with `load`
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
    cache::Bool
end

## TODO: Can make this an LRU cache
const _ondisk_cache = Dict{String, Any}()

function gather(ctx, d::OnDisk)
    if d.cache && haskey(_ondisk_cache, d.filename)
        _ondisk_cache[d.filename]
    else
        _ondisk_cache[d.filename] = unwrap_mmap(open(deserialize, d.filename))
    end
end

function save_as_chunk(data, filename_base; cache=true)
    jlsfile = filename_base * ".jls"
    mmapfile = filename_base * ".mmap"
    save_table(data, jlsfile, mmapfile)
    Dagger.Chunk(typeof(data), domain(data), OnDisk(jlsfile, cache), false)
end

function save_table(data::Table, file, mmap_file = file * ".mmap")
    ondiskdata = copy_mmap(mmap_file, data)
    open(io -> serialize(io, ondiskdata), file, "w")
end
