using JuliaDB

import Base: merge

# Core schema types

schema(xs::AbstractArray) = nothing # catch-all
merge(::Void, ::Void) = nothing
width(::Void) = 0
featuremat!(A, ::Void, xs) = A

# distributed schema calculation
function schema(xs::DArray)
    collect(treereduce(delayed(merge), delayedmap(schema, xs.chunks)))
end
schema(xs::ArrayOp) = schema(compute(xs))

struct Continuous
  series::Series
end
function schema(xs::AbstractArray{<:Real})
    Continuous(Series(xs, Mean(), Variance()))
end
width(::Continuous) = 1
function merge(c1::Continuous, c2::Continuous)
    Continuous(merge(c1.series, c2.series))
end
Base.mean(c::Continuous)::Float64 = value(c.series)[1]
Base.std(c::Continuous)::Float64 = c.series.stats[2].σ2
function Base.show(io::IO, c::Continuous)
    write(io, "Continous(μ=$(mean(c)), σ=$(std(c)))")
end
Base.@propagate_inbounds function featuremat!(A, c::Continuous, xs,
                                         dropna=Val{false}())
    m = mean(c)
    s = std(c)
    for i in 1:length(xs)
        x =  xs[i]
        if dropna isa Val{true}
            if isnull(x)
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
schema(xs::PooledArray) = Categorical(Series(xs, CountMap(eltype(xs))))
dict(c::Categorical) = c.series.stats[1].d
width(c::Categorical) = length(dict(c))
merge(c1::Categorical, c2::Categorical) = Categorical(merge(c1.series, c2.series))
function Base.show(io::IO, c::Categorical)
    write(io, "Categorical($(collect(keys(dict(c)))))")
end
Base.@propagate_inbounds function featuremat!(A, c::Categorical, xs, dropna=Val{false}())
    ks = collect(keys(dict(c)))
    labeldict = Dict{eltype(ks), Int}(zip(ks, 1:length(ks)))
    for i = 1:length(xs)
        if dropna isa Val{true} && isnull(xs[i])
            continue
        end
        A[i, labeldict[xs[i]]] = one(eltype(A))
    end
    A
end

struct Maybe{T}
  feature::T
end
schema(xs::DataValueArray) = Maybe(schema(dropna(xs)))
width(c::Maybe) = width(c.feature) + 1
merge(m1::Maybe, m2::Maybe) = Maybe(merge(m1.feature, m2.feature))
nulls(xs) = Base.Generator(isnull, xs)
nulls(xs::DataValueArray) = xs.isnull
Base.@propagate_inbounds function featuremat!(A, c::Maybe, xs, dropna=Val{true}())
    copy!(A, CartesianRange((1:length(xs), 1:1)), reshape(nulls(xs), (length(xs), 1)), CartesianRange((1:length(xs), 1:1)))
    featuremat!(view(A, 1:length(xs), 2:size(A, 2)), c.feature, xs, Val{true}())
end

# Schema inference

const Schema = Dict{Symbol,Any}

# vecTs: type of column vectors in each chunk
function schema(cols, names)
    d = Schema()
    for (col, name) in zip(cols, names)
        d[name] = schema(col)
    end
    d
end

function schema(t::Union{Dataset, DDataset})
    schema(collect(columns(t)), colnames(t))
end

width(sch::Schema) = sum(width(s) for s in values(sch))
function featuremat!(A, schemas::Schema, t)
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
    featuremat!(Array{Float32}(length(xs), width(sch)), sch, xs)
end
featuremat(t) = featuremat(schema(t), t)
function featuremat(s, t::DDataset)
    t = compute(t)
    w = width(s)
    h = length(t)
    lengths = get.(nrows.(t.domains))
    domains = Dagger.DomainBlocks((1,1), (cumsum(lengths), [w]))

    DArray(Float32,
           Dagger.ArrayDomain(sum(lengths), w),
           domains,
           reshape(delayedmap(x->featuremat(s,x), t.chunks),
                   (length(t.chunks), 1)))
end
