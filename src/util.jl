import IndexedTables: astuple

using NamedTuples

# re-export
export @NT

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

function treereduce(f, xs, v0=xs[1])
    length(xs) == 0 && return v0
    length(xs) == 1 && return xs[1]
    l = length(xs)
    m = div(l, 2)
    f(treereduce(f, xs[1:m]), treereduce(f, xs[m+1:end]))
end


function subtable(nds, r)
    Table(keys(nds)[r], values(nds)[r], presorted=true, copy=false)
end

function extrema_range(x::AbstractArray{T}, r::UnitRange) where T
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

"""
    load_table(file::AbstractString;
              indexcols, datacols, filenamecol, agg, presorted, copy, kwargs...)

Load a CSV file into an Table data. `indexcols` (AbstractArray)
specifies which columns form the index of the data, `datacols`
(AbstractArray) specifies which columns are to be used as the data.
`agg`, `presorted`, `copy` options are passed on to `Table`
constructor, any other keyword argument is passed on to `readcsv`

Returns an IndexedTable. A single implicit dimension with
values 1:N will be added if no `indexcols` (or `indexcols=[]`)
is specified.
"""
function load_table(args...; kwargs...)
    # just return the table
    _load_table(args...; kwargs...)[1]
end

function prettify_filename(f)
    f = basename(f)
    if endswith(lowercase(f), ".csv")
        f = f[1:end-4]
    end
    return f
end

function _load_table(file::Union{IO, AbstractString, AbstractArray}, delim=',';
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
        samecols = filter(x->isa(x, Union{Tuple, AbstractArray}),
                          indexcols)
    end
    if datacols !== nothing
        append!(samecols, filter(x->isa(x, Union{Tuple, AbstractArray}),
                                 datacols))
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
        if isa(file, AbstractArray)
            namecol = reduce(vcat, fill.(prettify_filename.(file), count))
        else
            namecol = fill(prettify_filename(file), length(cols[1]))
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
    _indexcols = map(x->lookupbyheader(header, x), indexcols)

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

        nullableidx = find(x->eltype(x) <: Nullable, indexvecs)
        if !isempty(nullableidx)
            badcol_names = header[_indexcols[nullableidx]]
            error("Indexed columns may not contain Nullables. Column(s) with nullables: $(join(badcol_names, ", ", " and "))")
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
            fill(Nullable{Union{}}(), n) # missing column
        else
            cols[i]
        end
    end

    data = Columns(datavecs...; names=datacolnames)

    Table(index, data), implicitindex
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

using PooledArrays
using NullableArrays

"""
    MmappableArray

A convenience wrapper that can be used to pass around metadata about
an Mmapped array.

When serialized, this only writes the metadata and leaves out the data.

copy_mmap(io::IO, file::String, x::T) # =>
"""
mutable struct MmappableArray{T, N, A} <: AbstractArray{T, N}
    file::String
    offset::Int
    size::NTuple{N, Int}
    data::A
end

@inline Base.size(arr::MmappableArray) = arr.size
@inline Base.getindex(arr::MmappableArray, idx...) = arr.data[idx...]
Base.IndexStyle{T,N,A}(::Type{MmappableArray{T,N,A}}) = Base.IndexStyle(A)

function Base.similar(A::M, sz::Int...) where M<:MmappableArray
    # this is to keep Table constructor happy
    M("__unmmapped__", 0, sz, similar(A.data, sz...))
end

function Base.similar(pa::PooledArray{T,R,N,M}, S::Type, dims::Dims) where {T,R,N,M<:MmappableArray}
    z = M("__unmmapped__", 0, dims, zeros(R, dims))
    PooledArray(PooledArrays.RefArray(z), Dict{S, R}())
end

# construct an MmappableArray from a normal array, writing it to file
function MmappableArray(io::IO, file::String, data::Array{T}) where T
    if !isbits(T)
        error("Cannot Mmap non-bits array $T")
    end

    offset = position(io)
    arr = Mmap.mmap(io, typeof(data), size(data), offset)
    copy!(arr, data)
    Mmap.sync!(arr)
    seek(io, offset+sizeof(data))
    MmappableArray{T, ndims(data), typeof(data)}(file, offset, size(data), arr)
end

function MmappableArray(io::IO, file::String, data::PooledArray)
    mmaprefs = MmappableArray(io, file, data.refs)
    PooledArray(PooledArrays.RefArray(mmaprefs), data.pool)
end

function MmappableArray(file::String, data)
    open(file, "w+") do io
        MmappableArray(io, file, data)
    end
end

# Load an Mmap array from file
function MmappableArray(file::String, ::Type{A}, offset::Int, dims) where A
    MmappableArray{eltype(A), ndims(A), A}(file, offset, dims, Mmap.mmap(file, A, dims, offset))
end

function Base.serialize(io::AbstractSerializer, arr::MmappableArray)
    Mmap.sync!(arr.data)
    Base.Serializer.serialize_type(io, typeof(arr))
    Base.serialize(io, (arr.file, arr.offset, arr.size))
    if arr.file == "__unmmapped__"
        Base.serialize(io, arr.data)
    end
end

function Base.deserialize(io::AbstractSerializer, ::Type{MmappableArray{T,N,A}}) where {T, N, A}
    (file, offset, dims)  = deserialize(io)
    if file == "__unmmapped__"
        data = deserialize(io)
        return MmappableArray{T,N,A}(file, offset, dims, data)
    end
    MmappableArray(file, A, offset, dims)
end

using IndexedTables

# assuming we don't use non-bits numbers
const MmappableTypes = Union{Integer, AbstractFloat, Complex, Char, DateTime, Date}

### Wrap arrays in Mmappable wrapper. Do this before serializing
copy_mmap(io, file, arr::PooledArray) = MmappableArray(io, file, arr)
copy_mmap(io, file, arr::Array{T}) where {T<:MmappableTypes} = MmappableArray(io, file, arr)
function copy_mmap(io, file, arr::Columns)
    cs = map(x->copy_mmap(io, file, x), arr.columns)
    if all(x->isa(x, Int), fieldnames(cs))
        Columns(cs...)
    else
        # NamedTuple case
        Columns(cs...; names=fieldnames(cs))
    end
end
copy_mmap(io, file, arr::AbstractArray) = arr

## This must be called after sorting and aggregation!
function copy_mmap(io::IO, file::String, nds::Table)
    flush!(nds)
    index = copy_mmap(io, file, nds.index)
    data  = copy_mmap(io, file, nds.data)
    Table(index, data, copy=false, presorted=true)
end

function copy_mmap(file::String, data)
    open(file, "w+") do io
        copy_mmap(io, file, data)
    end
end

### Unwrap

# we should unwrap before processing because MmappableArray is
# not a complete array wrapper. We keep track of which arrays were
# part of which wrapper. If an array remains unchanged after it is
# deserialized by a proc, we can wrap it in MmappableArray before sending it
function unwrap_mmap(arr::MmappableArray)
    #track[arr.data] = (arr.file, arr.offset, arr.size)
    arr.data
end

function unwrap_mmap(arr::PooledArray)
    refs = unwrap_mmap(arr.refs)
    PooledArray(PooledArrays.RefArray(refs), arr.pool)
end

function unwrap_mmap(arr::Columns)
    cs = map(x->unwrap_mmap(x), arr.columns)
    if all(x->isa(x, Int), fieldnames(cs))
        Columns(cs...)
    else
        # NamedTuple case
        Columns(cs...; names=fieldnames(cs))
    end
end

function unwrap_mmap(arr::Table)
    Table(unwrap_mmap(arr.index), unwrap_mmap(arr.data), presorted=true, copy=false)
end

unwrap_mmap(arr::AbstractArray) = arr


function _repeated(x, n)
    Iterators.repeated(x,n)
end


import Dagger: approx_size

function approx_size(cs::Columns)
    sum(map(approx_size, astuple(cs.columns)))
end

function approx_size(t::IndexedTable)
    approx_size(t.data) + approx_size(t.index)
end

using PooledArrays

function approx_size(pa::PooledArray)
    approx_size(pa.refs) + approx_size(pa.pool)
end

# smarter merges on NullableArray + other arrays
import IndexedTables: promoted_similar

function promoted_similar(x::NullableArray, y::NullableArray, n)
    similar(x, promote_type(eltype(x),eltype(y)), n)
end

function promoted_similar(x::NullableArray, y::AbstractArray, n)
    similar(x, promote_type(eltype(x),eltype(y)), n)
end

function promoted_similar(x::AbstractArray, y::NullableArray, n)
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
