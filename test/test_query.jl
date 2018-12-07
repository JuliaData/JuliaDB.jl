@testset "map & reduce" begin
    t = NDSparse(Columns([1,1,2,2], [1,2,1,2]), [1,2,3,4])
    d = distribute(t, 2)
    @test collect(map(-, d)) == map(-, t)
    @test reduce(+, d) == 10
end

@testset "getindex" begin
    t = NDSparse(Columns(x=[1,1,1,2,2], y=[1,2,3,1,2]), [1,2,3,4,5])
    for n=1:5
        d = distribute(t, n)

        @test d[1,1] == t[1,1]
        @test d[1,3] == t[1,3]
        @test d[2,2] == t[2,2]

        @test collect(d[1:1, 1:1]) == t[1:1, 1:1]
        @test collect(d[1:2, 2:3]) == t[1:2, 2:3]
        @test collect(d[:, 3]) == t[:, 3]
    end
end

@testset "select" begin
    t = NDSparse(Columns(a=[1,1,1,2,2], b=[1,2,3,1,2]), [1,2,3,4,5])
    for i=[1, 3, 5]
        d = distribute(t, i)

        res = filter((1=>x->true, 2=>x->x%2 == 0), t)
        @test collect(filter((1=>x->true, 2=>x->x%2 == 0), d)) == res
        @test collect(filter((:a=>x->true, :b => x->x%2 == 0), d)) == res

        # check empty, #228
        res = filter((1=>x->false, 2=>x->x%2 == 0), t)
        @test collect(filter((1=>x->false, 2=>x->x%2 == 0), d)) == res
        @test collect(filter((:a=>x->false, :b => x->x%2 == 0), d)) == res

    end
end

function Base.isapprox(x::NDSparse, y::NDSparse)
    flush!(x); flush!(y)
    all(map(isapprox, x.data.columns, y.data.columns))
end

@testset "convertdim" begin

    t = NDSparse(Columns(a=[1,1,1,2,2], b=[1,2,3,1,2]),
                     Columns(c=[1,2,3,4,5], d=[5,4,3,2,1]))

    _plus(x,y) = map(+,x, y)

    for i=[2, 3, 5]
        d = distribute(t, i)
        @test collect(convertdim(d, 2, x->x>=2)) == convertdim(t, 2, x->x>=2)
        @test collect(convertdim(d, 2, x->x>=2, agg=_plus)) == convertdim(t, 2, x->x>=2, agg=_plus)
        @test collect(convertdim(d, 2, x->x>=2, vecagg=length)) ==
                convertdim(t, 2, x->x>=2, vecagg=length)
    end
end

@testset "reduce" begin
    t1 = NDSparse(Columns([1,1,2,2], [1,2,1,2]), [1,2,3,4])
    rd1 = reduce(+, t1, dims=1)
    rd2 = reduce(+, t1, dims=2)
    rdv1 = reducedim_vec(length, t1, 1)
    rdv2 = reducedim_vec(length, t1, 2)

    for n=1:5
        d1 = distribute(t1, n, allowoverlap=true)
        @test collect(reduce(+, d1, dims=1)) == rd1
        @test collect(reduce(+, d1, dims=2)) == rd2

        @test collect(reducedim_vec(length, d1, 1)) == rdv1
        @test collect(reducedim_vec(length, d1, 2)) == rdv2
    end
end

@testset "select" begin
    t1 = NDSparse(Columns([1,2,3,4], [2,1,1,2]), [1,2,3,4])
    d1 = distribute(t1, 2)
    @test collect(selectkeys(d1, 2, agg=+)) == selectkeys(t1, 2, agg=+)
end

@testset "permutedims" begin
    t = NDSparse(Columns([1,1,2,2], ["a","b","a","b"]), [1,2,3,4])
    for n=1:5
        d = distribute(t, n)
        @test collect(permutedims(d, [2,1])) == permutedims(t, [2,1])
    end
end

@testset "mapslices" begin
    t = NDSparse(Columns(x=[1,1,2,2], y=[1,2,3,4]), [1,2,3,4])

    for (dist,) in zip(Any[1, 2, [1,3], [3,1], [1,2,1]])
        d = distribute(t, dist)
        res = mapslices(collect, d, :y)
        @test collect(res) == mapslices(collect, t, 2)
        f = x->IndexedTables.NDSparse(IndexedTables.Columns(z=[1,2]), [3,4])
        res2 = mapslices(f, d, 2)
        @test collect(res2) == mapslices(f, t, 2)
    end
    t = ndsparse([1,2,3],[4,5,6], chunks=2)
    @test mapslices(t, ()) do x
        IndexedTables.ndsparse([1,2], first.([x,x]))
    end == ndsparse(([1,1,2,2,3,3],[1,2,1,2,1,2],), [4,4,5,5,6,6])
end

@testset "flatten" begin
    x = table([1,2], [[3,4], [5,6]], names=[:x, :y], chunks=2)
    @test flatten(x, 2) == table([1,1,2,2], [3,4,5,6], names=[:x,:y])

    x = table([1,2], [table([3,4],[5,6], names=[:a,:b]), table([7,8], [9,10], names=[:a,:b])], names=[:x, :y], chunks=2)
    @test flatten(x, :y) == table([1,1,2,2], [3,4,7,8], [5,6,9,10], names=[:x,:a, :b])

    t = table([1,1,2,2], [3,4,5,6], names=[:x,:y], chunks=2)
    @test groupby((:normy => x->Iterators.repeated(mean(x), length(x)),),
                  t, :x, select=:y, flatten=true) == table([1,1,2,2], [3.5,3.5,5.5,5.5], names=[:x, :normy])
end
