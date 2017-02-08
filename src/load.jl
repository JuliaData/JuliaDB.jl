using TextParse
using IndexedTables
using Glob

export @dateformat_str, load, loadNDSparse, glob

function getsubset(cols, subcols)
    idx = length(subcols) > 1 ?
        Columns(map(i -> cols[i], subcols)...) : Columns(cols[subcols[1]])
end

"""
    loadNDSparse(file::AbstractString;
                 indexcols, datacols, agg, presorted, copy, kwargs...)

Load a CSV file into an NDSparse data. `indexcols` (AbstractArray)
specifies which columns form the index of the data, `datacols`
(AbstractArray) specifies which columns are to be used as the data.
`agg`, `presorted`, `copy` options are passed on to `NDSparse`
constructor, any other keyword argument is passed on to `readcsv`
"""
function loadNDSparse(file::AbstractString;
                      indexcols=Int[],
                      datacols=-1,
                      agg=nothing,
                      presorted=false,
                      copy=false,
                      kwargs...)

    println("LOADING ", file)
    cols,header = csvread(file; kwargs...)
    if datacols == -1
        # last column
        datacols = length(cols)
    end

    if isempty(indexcols)
        # all columns that aren't data
        indexcols = [x for x in 1:length(cols) if !(x in datacols)]
    end

    NDSparse(getsubset(cols, indexcols), getsubset(cols, datacols))
end

immutable ChunkInfo
    filename::String
    typeof::Type
    length::Int
    domain::TableDomain
end

# Get the ChunkInfo from a filename and the data in it
# this is run on the workers
function chunkinfo(file::String, data::NDSparse)
    ChunkInfo(file,
              typeof(data),
              length(data),
              domain(data))
end

comp(f,g) = (x...) -> f(g(x...))

# Input: a vector of `TableDomain`s
# Output: a DomainSplit with the TableDomains
function combine_domains(ds)
    # Overall domain of combined chunks:
    fst = minimum(map(x->first(x.interval), ds))
    lst = maximum(map(x->last(x.interval), ds))
    head = TableDomain(fst, lst)

    # Domains of each part in an IntervalTree
    #parts = TableDomainSet(ds)

    DomainSplit(head, ds)
end

function load(files::AbstractVector; opts...)
    # Load the data first into memory
    data = map(tothunk(f -> loadNDSparse(f; opts...), persist=true), files)

    # create ChunkInfo from the data
    metadata = map(tothunk(chunkinfo), files, data)

    # Give an idea of what we're up against, we should probably also show a
    # progress meter.
    sz = sum(map(filesize, files))
    println("Loading $(length(files)) csv files totalling $(round(sz/2^20)) MB...")

    # Gather ChunkInfo for each chunk
    chunks_meta = gather(Dagger.treereduce(tothunk(vcat), metadata))

    # TODO: make this work when the types are different.
    table_type = reduce(promote_type, map(x->x.typeof, chunks_meta))
    table_domain = combine_domains(map(x->x.domain, chunks_meta))

    # TODO: this should be read in from Parquet files (saved from step 1)
    data_saved = data # for now

    DTable(Cat(table_type, table_domain, data_saved))
end
