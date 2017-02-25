
function subtable(nds, r)
    NDSparse(nds.index[r], nds.data[r])
end

"""
get a subset of vectors wrapped in Columns from a tuple of vectors
"""
function getcolsubset(cols, subcols)
    idx = length(subcols) > 1 ?
        Columns(map(i -> cols[i], subcols)...) : Columns(cols[subcols[1]])
end

# Data loading utilities
using TextParse
using Glob

export @dateformat_str, load, csvread, loadNDSparse, glob

"""
    loadNDSparse(file::AbstractString;
                 indexcols, datacols, agg, presorted, copy, kwargs...)

Load a CSV file into an NDSparse data. `indexcols` (AbstractArray)
specifies which columns form the index of the data, `datacols`
(AbstractArray) specifies which columns are to be used as the data.
`agg`, `presorted`, `copy` options are passed on to `NDSparse`
constructor, any other keyword argument is passed on to `readcsv`
"""
function loadNDSparse(file::AbstractString, delim=',';
                      indexcols=Int[],
                      datacols=-1,
                      agg=nothing,
                      presorted=false,
                      copy=false,
                      csvread=TextParse.csvread,
                      kwargs...)

    #println("LOADING ", file)
    cols,header = csvread(file, delim; kwargs...)
    if datacols == -1
        # last column
        datacols = length(cols)
    end

    if isempty(indexcols)
        # all columns that aren't data
        indexcols = [x for x in 1:length(cols) if !(x in datacols)]
    end

    NDSparse(getcolsubset(cols, indexcols), getcolsubset(cols, datacols))
end
