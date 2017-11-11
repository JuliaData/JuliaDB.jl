using JuliaDB
using Base.Test

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
        # FIXME
        @test_throws ErrorException collect(d[1:2, 4:3])
        @test collect(d[:, 3]) == t[:, 3]
    end
end

@testset "select" begin

    t = NDSparse(Columns(a=[1,1,1,2,2], b=[1,2,3,1,2]), [1,2,3,4,5])
    for i=[1, 3, 5]
        d = distribute(t, i)

        res = select(t, 1=>x->true, 2=>x->x%2 == 0)
        @test collect(select(d, 1=>x->true, 2=>x->x%2 == 0)) == res
        @test collect(select(d, :a=>x->true, :b => x->x%2 == 0)) == res
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

@testset "reducedim" begin
    t1 = NDSparse(Columns([1,1,2,2], [1,2,1,2]), [1,2,3,4])
    rd1 = reducedim(+, t1, 1)
    rd2 = reducedim(+, t1, 2)
    rdv1 = reducedim_vec(length, t1, 1)
    rdv2 = reducedim_vec(length, t1, 2)

    for n=1:5
        d1 = distribute(t1, n, allowoverlap=true)
        @test collect(reducedim(+, d1, 1)) == rd1
        @test collect(reducedim(+, d1, 2)) == rd2

        @test collect(reducedim_vec(length, d1, 1)) == rdv1
        @test collect(reducedim_vec(length, d1, 2)) == rdv2
    end
end

@testset "select" begin
    t1 = NDSparse(Columns([1,2,3,4], [2,1,1,2]), [1,2,3,4])
    d1 = distribute(t1, 2)
    @test collect(select(d1, 2, agg=+)) == select(t1, 2, agg=+)
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
        f = x->NDSparse(Columns(z=[1,2]), [3,4])
        res2 = mapslices(f, d, 2)
        @test collect(res2) == mapslices(f, t, 2)
        # uncomment when breaking API for mapslices () is released
        #g = x->NDSparse(Columns(z=[1,2]), [x[1][1],x[2]])
        #res3 = mapslices(g, d, ())
        #@test collect(res3) == mapslices(g, t, ())
    end
end
