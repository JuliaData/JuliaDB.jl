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
function load(files::AbstractVector; opts...)
    # Give an idea of what we're up against, we should probably also show a
    # progress meter.
    sz = sum(map(filesize, files))
    println("Loading $(length(files)) csv files totalling $(round(sz/2^10)) kB...")

    # Load the data first into memory
    data = map(file -> Thunk(f -> loadNDSparse(f; opts...), file, persist=true), files)

    chunks = compute(Thunk(data...; meta=true) do cs...
            # TODO: this should be read in from Parquet files (saved from step 1)
            # right now we are just caching it in memory...
            [cs...]
        end)

    fromchunks(chunks)
end
