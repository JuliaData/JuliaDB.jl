import IndexedTables: astuple
using NamedTuples

using PooledArrays
using DataValues
using WeakRefStrings


# re-export
export @NT

function treereduce(f, xs, v0=xs[1])
    length(xs) == 0 && return v0
    length(xs) == 1 && return xs[1]
    l = length(xs)
    m = div(l, 2)
    f(treereduce(f, xs[1:m]), treereduce(f, xs[m+1:end]))
end


function subtable(nds::NDSparse, r)
    NDSparse(keys(nds)[r], values(nds)[r], presorted=true, copy=false)
end

function subtable(t::NextTable, r)
    t[r]
end

function extrema_range(x::AbstractArray{T}, r::UnitRange) where T
    return first(x), last(x)
    if !(1 <= first(r) && last(r) <= length(x))
        throw(BoundsError(x, r))
    end

    isempty(r) && return extrema(x[r])
    mn = x[first(r)]
    mx = x[first(r)]
    @inbounds @simd for i in r
        mn = min(x[i], mn)
        mx = max(x[i], mx)
    end
    mn, mx
end

# Data loading utilities
using TextParse
using Glob

export @dateformat_str, load, csvread, load_table, glob

Base.@deprecate load_table(args...;kwargs...) loadndsparse(args...; distributed=false, kwargs...)

function prettify_filename(f)
    f = basename(f)
    if endswith(lowercase(f), ".csv")
        f = f[1:end-4]
    end
    return f
end

function _loadtable_serial(T, file::Union{IO, AbstractString, AbstractArray};
                      delim=',',
                      indexcols=[],
                      datacols=nothing,
                      filenamecol=nothing,
                      agg=nothing,
                      presorted=false,
                      copy=false,
                      csvread=TextParse.csvread,
                      kwargs...)

    #println("LOADING ", file)
    count = Int[]

    samecols = nothing
    if indexcols !== nothing
        if indexcols isa Union{Int, Symbol}
            indexcols = (indexcols,)
        end
        samecols = collect(Iterators.filter(x->isa(x, Union{Tuple, AbstractArray}),
                                            indexcols))
    end
    if datacols !== nothing
        if datacols isa Union{Int, Symbol}
            datacols = (datacols,)
        end
        append!(samecols, collect(Iterators.filter(x->isa(x, Union{Tuple, AbstractArray}),
                                                   datacols)))
    end

    if samecols !== nothing
        samecols = map(x->map(string, x), samecols)
    end

    if isa(file, AbstractArray)
        cols, header, count = csvread(file, delim;
                                      samecols=samecols,
                                      kwargs...)
    else
        cols, header = csvread(file, delim; kwargs...)
    end

    header = map(string, header)

    if filenamecol !== nothing
        # mimick a file name column
        if filenamecol isa Pair
            filenamecol, f = filenamecol
        else
            f = prettify_filename
        end
        if isa(file, AbstractArray)
            namecol = reduce(vcat, fill.(f.(file), count))
        else
            namecol = fill(f(file), length(cols[1]))
        end

        cols = (namecol, cols...)
        if !isempty(header)
            unshift!(header, string(filenamecol))
        end
    end

    if isempty(cols)
        error("File contains no columns!")
    end

    n = length(first(cols))
    implicitindex = false

    ## Construct Index
    _indexcols = collect(map(x->lookupbyheader(header, x), indexcols))

    if isempty(_indexcols)
        implicitindex = true
        index = Columns([1:n;])
    else
        indexcolnames = map(indexcols, _indexcols) do name, i
            if i==0
                error("Cannot index by unknown column $name")
            else
                isa(name, Int) ? canonical_name(header[name]) : canonical_name(name)
            end
        end

        indexvecs = cols[_indexcols]

        nullableidx = find(x->eltype(x) <: Union{DataValue,Nullable}, indexvecs)
        if !isempty(nullableidx)
            badcol_names = header[_indexcols[nullableidx]]
            warn("Indexed columns may contain Nullables or NAs. Column(s) with nullables: $(join(badcol_names, ", ", " and ")). This will result in wrong sorting.")
        end

        index = Columns(indexvecs...; names=indexcolnames)
    end

    ## Construct Data
    if datacols === nothing
        _datacols = setdiff(1:length(cols), _indexcols)
        datacols = header[_datacols]
    else
        _datacols = map(x->lookupbyheader(header, x), datacols)
    end

    if isempty(_datacols)
        error("""You must specify at least one data column.
                 Either all columns in the file were indexed, or datacols was explicitly set to an empty array.""")
    end

    datacolnames = map(datacols, _datacols) do name, i
        if i == 0
            if isa(name, Int)
                error("Unknown column numbered $name specified in datacols")
            else
                return canonical_name(name) # use provided name for missing column
            end
        else
            isa(name, Int) ? canonical_name(header[name]) : canonical_name(name)
        end
    end

    datavecs = map(_datacols) do i
        if i == 0
            DataValueArray{Union{}}(n) # missing column
        else
            cols[i]
        end
    end

    data = Columns(datavecs...; names=datacolnames)

    if T<:NextTable && implicitindex
        table(data, copy = copy), true
    else
        convert(T, index, data, copy = copy), implicitindex
    end
end

function lookupbyheader(header, key)
    if isa(key, Symbol)
        return lookupbyheader(header, string(key))
    elseif isa(key, String)
        return findfirst(x->x==key, header)
    elseif isa(key, Int)
        return 0 < key <= length(header) ? key : 0
    elseif isa(key, Tuple) || isa(key, Vector)
        for k in key
            x = lookupbyheader(header, k)
            x != 0 && return x
        end
        return 0
    end
end

canonical_name(n::Symbol) = n
canonical_name(n::String) = Symbol(replace(n, r"\s", "_"))
canonical_name(n::Union{Tuple, Vector}) = canonical_name(first(n))

function _repeated(x, n)
    Iterators.repeated(x,n)
end


import MemPool: approx_size

function approx_size(cs::Columns)
    sum(map(approx_size, astuple(cs.columns)))
end

function approx_size(t::NDSparse)
    approx_size(t.data) + approx_size(t.index)
end

using PooledArrays

function approx_size(pa::PooledArray)
    approx_size(pa.refs) + approx_size(pa.pool) * 2
end

function approx_size(t::NextTable)
    approx_size(rows(t))
end

function approx_size(x::StringArray)
    approx_size(x.buffer) + approx_size(x.offsets) + approx_size(x.lengths)
end

# smarter merges on DataValueArray + other arrays
import IndexedTables: promoted_similar

function promoted_similar(x::DataValueArray, y::DataValueArray, n)
    similar(x, promote_type(eltype(x),eltype(y)), n)
end

function promoted_similar(x::DataValueArray, y::AbstractArray, n)
    similar(x, promote_type(eltype(x),eltype(y)), n)
end

function promoted_similar(x::AbstractArray, y::DataValueArray, n)
    similar(y, promote_type(eltype(x),eltype(y)), n)
end

# The following is not inferable, this is OK because the only place we use
# this doesn't need it.

function _map_params(f, T, S)
    (f(_tuple_type_head(T), _tuple_type_head(S)), _map_params(f, _tuple_type_tail(T), _tuple_type_tail(S))...)
end

_map_params(f, T::Type{Tuple{}},S::Type{Tuple{}}) = ()

map_params(f, ::Type{T}, ::Type{S}) where {T,S} = f(T,S)
@inline _tuple_type_head{T<:Tuple}(::Type{T}) = Base.tuple_type_head(T)
@inline _tuple_type_tail{T<:Tuple}(::Type{T}) = Base.tuple_type_tail(T)

#function map_params{N}(f, T::Type{T} where T<:Tuple{Vararg{Any,N}}, S::Type{S} where S<: Tuple{Vararg{Any,N}})
Base.@pure function map_params(f, ::Type{T}, ::Type{S}) where {T<:Tuple,S<:Tuple}
    if nfields(T) != nfields(S)
        MethodError(map_params, (typeof(f), T,S))
    end
    Tuple{_map_params(f, T,S)...}
end

_tuple_type_head(T::Type{NT}) where {NT<: NamedTuple} = fieldtype(NT, 1)

Base.@pure function _tuple_type_tail(T::Type{NT}) where NT<: NamedTuple
    Tuple{Base.argtail(NT.parameters...)...}
end

Base.@pure @generated function map_params(f, ::Type{T}, ::Type{S}) where {T<:NamedTuple,S<:NamedTuple}
    if fieldnames(T) != fieldnames(S)
        MethodError(map_params, (T,S))
    end
    NT = Expr(:macrocall, :(NamedTuples.$(Symbol("@NT"))), fieldnames(T)...)
    :($NT{_map_params(f, T, S)...})
end

function randomsample(n, r::Range)
    k = 0
    taken = Set{eltype(r)}()
    output = eltype(r)[]
    while k < n && k < length(r)
        x = rand(r)
        if !(x in taken)
            push!(taken, x)
            push!(output, x)
            k += 1
        end
    end
    return output
end

function tuplesetindex(x::Tuple{Vararg{Any,N}}, v, i) where N
    ntuple(Val{N}) do j
        i == j ? v : x[j]
    end
end

@generated function tuplesetindex(x::NamedTuple, v, i::Symbol)
    fields = fieldnames(x)
    :(@NT($(fields...))(tuplesetindex(x, v, findfirst($fields, i))...))
end

@generated function tuplesetindex(x::NamedTuple, v, i::Int)
    fields = fieldnames(x)
    N = length(fields)
    quote
        tup = Base.@ntuple $N j -> i == j ? v : x[j]
        @NT($(fields...))(tuplesetindex(tup, v, i)...)
    end
end

function tuplesetindex(x::Union{NamedTuple, Tuple}, v::Tuple, i::Tuple)
    reduce((t, j)->tuplesetindex(t, v[j], i[j]), x, 1:length(i))
end


# mild piracy
Base.similar(::PooledArrays.PooledArray, ::Type{DataValue{T}}, sz::Tuple{Vararg{Int64,N}}) where {T,N} = DataValueArray{T}(sz)
