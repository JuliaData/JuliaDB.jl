
using NamedTuples

function Base.isless(t1::NamedTuple, t2::NamedTuple)
    n1, n2 = length(t1), length(t2)
    for i = 1:min(n1, n2)
        a, b = t1[i], t2[i]
        if !isequal(a, b)
            return isless(a, b)
        end
    end
    return n1 < n2
end

@generated function Base.map(f, nts::NamedTuple...)
    fields = fieldnames(nts[1])
    if !all(map(x->isequal(fieldnames(x), fields), nts[2:end]))
        throw(ArgumentError("All NamedTuple inputs to map must have the same fields"))
    end
    N = length(fields)
    args = ntuple(N) do i
        :($(fields[i]) => f(map(t->t[$i], nts)...))
    end
    :(@NT($(args...)))
end

function subtable(nds, r)
    NDSparse(nds.index[r], nds.data[r])
end

getbyheader(cols, header, i::Int) = cols[i]
getbyheader(cols, header, i::Symbol) = getcol(cols, header, string(i))
function getbyheader(cols, header, i::AbstractString)
    if !(i in header)
        throw(ArgumentError("Unknown column $i"))
    end
    getbyheader(cols, header, findfirst(header, i))
end

"""
get a subset of vectors wrapped in Columns from a tuple of vectors
"""
function getcolsubset(cols, header, subcols)
    colnames = !isempty(header) ?
        vcat(map(i -> Symbol(getbyheader(header, header, i)), subcols)) :
        nothing

    if length(subcols) > 1
        Columns(map(i -> getbyheader(cols, header, i), subcols)...; names=colnames)
    else
        Columns(getbyheader(cols, header, subcols[1]); names=colnames)
    end
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
                      datacols=Int[],
                      agg=nothing,
                      presorted=false,
                      copy=false,
                      csvread=TextParse.csvread,
                      kwargs...)

    #println("LOADING ", file)
    cols,header = csvread(file, delim; kwargs...)

    if isempty(indexcols) && isempty(datacols)
        indexcols = 1:(length(cols)-1)
        datacols  = [length(cols)]
    end

    if isempty(indexcols)
        # all columns that aren't data
        _datacols = map(i->getbyheader(1:length(cols), header, i), datacols)
        indexcols = [x for x in 1:length(cols) if !(x in _datacols)]
    end

    if isempty(datacols)
        # all columns that aren't index
        _indexcols = map(i->getbyheader(1:length(cols), header, i), indexcols)
        datacols = [x for x in 1:length(cols) if !(x in _indexcols)]
    end

    index = getcolsubset(cols, header, indexcols)
    data = getcolsubset(cols, header, datacols)
    NDSparse(index, data)
end
