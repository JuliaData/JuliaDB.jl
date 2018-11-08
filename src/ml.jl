module ML

using JuliaDB
using Dagger
using OnlineStats
using PooledArrays
using DataValues
using Statistics

import Dagger: ArrayOp, DArray, treereduce
import JuliaDB: Dataset, DDataset, nrows

import Base: merge

# Core schema types

schema(xs::AbstractArray) = nothing # catch-all
schema(xs::AbstractArray, ::Nothing) = nothing # catch-all
merge(::Nothing, ::Nothing) = nothing
width(::Nothing) = 0
featuremat!(A, ::Nothing, xs) = A

schema(xs::ArrayOp) = schema(compute(xs))
schema(xs::ArrayOp, T) = schema(compute(xs), T)

struct Continuous
  series::Series
end

function schema(xs, ::Type{Continuous})
  Continuous(fit!(Series(Variance()),xs))
end
function schema(xs::AbstractArray{<:Real})
    schema(xs, Continuous)
end
width(::Continuous) = 1
function merge(c1::Continuous, c2::Continuous)
    Continuous(merge(c1.series, c2.series))
end
Statistics.mean(c::Continuous)::Float64 = mean(c.series.stats[1])
Statistics.std(c::Continuous)::Float64 = std(c.series.stats[1])
function Base.show(io::IO, c::Continuous)
    write(io, "Continous(μ=$(mean(c)), σ=$(std(c)))")
end
Base.@propagate_inbounds function featuremat!(A, c::Continuous, xs,
                                         dropna=Val(false))
    m = mean(c)
    s = std(c)
    for i in 1:length(xs)
        x =  xs[i]
        if dropna isa Val{true}
            if isna(x)
                continue
            else
                A[i, 1] = (get(x) - m) / s
            end
        else
            A[i, 1] = (x - m) / s
        end
    end
    A
end


struct Categorical
  series::Series
end
function Categorical(xs::AbstractArray)
    Categorical(Series(CountMap(Dict(zip(xs, zeros(Int, length(xs)))))))
end

function schema(xs::AbstractArray, ::Type{Categorical})
  Categorical(fit!(Series(CountMap(eltype(xs))),xs))
end

function schema(xs::PooledArray)
    schema(xs, Categorical)
end

Base.keys(c::Categorical) = keys(c.series.stats[1])
width(c::Categorical) = length(keys(c))
merge(c1::Categorical, c2::Categorical) = Categorical(merge(c1.series, c2.series))
function Base.show(io::IO, c::Categorical)
    write(io, "Categorical($(collect(keys(c))))")
end
Base.@propagate_inbounds function featuremat!(A, c::Categorical, xs, dropna=Val(false))
    ks = keys(c)
    labeldict = Dict{eltype(ks), Int}(zip(ks, 1:length(ks)))
    for i = 1:length(xs)
        if dropna isa Val{true} && isna(xs[i])
            continue
        end
        A[i, labeldict[xs[i]]] = one(eltype(A))
    end
    A
end

# distributed schema calculation
function schema(xs::DArray)
    collect(treereduce(delayed(merge),
                       delayedmap(x -> schema(x), xs.chunks)))
end

for S in (Continuous, Categorical)
    @eval function schema(xs::DArray, T::Type{$S})
        collect(treereduce(delayed(merge),
                           delayedmap(x -> schema(x, T), xs.chunks)))
    end
end

struct Maybe{T}
  feature::T
end
function schema(xs::DataValueArray, T::Type)
    Maybe(schema(dropna(xs), T))
end
schema(xs::DataValueArray) = Maybe(schema(dropna(xs)))
width(c::Maybe) = width(c.feature) + 1
merge(m1::Maybe, m2::Maybe) = Maybe(merge(m1.feature, m2.feature))
nulls(xs) = Base.Generator(isna, xs)
nulls(xs::DataValueArray) = xs.isna
Base.@propagate_inbounds function featuremat!(A, c::Maybe, xs, dropna=Val(true))
    copyto!(A, CartesianIndices((1:length(xs), 1:1)), reshape(nulls(xs), (length(xs), 1)), CartesianIndices((1:length(xs), 1:1)))
    featuremat!(view(A, 1:length(xs), 2:size(A, 2)), c.feature, xs, Val(true))
    A
end

# Schema inference

const Schema = Dict{Symbol,Any}

# vecTs: type of column vectors in each chunk
function schema(cols, names; hints=Dict())
    d = Schema()
    for (col, name) in zip(cols, names)
        if haskey(hints, name)
            d[name] = schema(col, hints[name])
        else
            d[name] = schema(col)
        end
    end
    d
end

function schema(t::Union{Dataset, DDataset}; hints=Dict())
    schema(collect(columns(t)), colnames(t), hints=hints)
end

width(sch::Schema) = sum(width(s) for s in values(sch))
function featuremat!(A, schemas::Schema, t::Dataset)
    j = 0
    for col in keys(schemas)
        schema = schemas[col]
        featuremat!(view(A, 1:length(t), j+1:j+width(schema)),
                   schema, column(t, col))

        j += width(schema)
    end
    A
end

splitschema(xs::Schema, ks...) =
    filter((k,v) -> k ∉ ks, xs),
    filter((k,v) -> k ∈ ks, xs)

function featuremat(sch, xs)
    featuremat!(zeros(Float32, length(xs), width(sch)), sch, xs)'
end
featuremat(t) = featuremat(schema(t), t)

function featuremat(s, t::DDataset)
    t = compute(t)
    w = width(s)
    h = length(t)
    lengths = get.(nrows.(t.domains))
    domains = Dagger.DomainBlocks((1,1), ([w], cumsum(lengths)))

    DArray(Float32,
           Dagger.ArrayDomain(w, sum(lengths)),
           domains,
           reshape(delayedmap(x->featuremat(s,x), t.chunks),
                   (1, length(t.chunks))))
end

end
