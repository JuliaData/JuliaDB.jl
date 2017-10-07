using Base.Test
using JuliaDB

@testset "iteration" begin
    x = distribute(IndexedTable(Columns(a=[1,1], b=[1,2]), Columns(c=[3,4])), 2)
    y = distribute(IndexedTable(Columns(a=[1,1], b=[1,2]), [3,4]), 2)

    @test column(x, :a) == [1,1]
    @test column(x, :b) == [1,2]
    @test column(x, :c) == [3,4]
    @test column(x, 3) == [3,4]
    @test column(y, 3) == [3,4]

    @test columns(x) == @NT(a=[1,1], b=[1,2], c=[3,4])
    @test columns(x, (:a,:c)) == @NT(a=[1,1], c=[3,4])
    @test columns(y, (1, 3)) == ([1,1], [3,4])

    @test rows(x) == [@NT(a=1,b=1,c=3), @NT(a=1,b=2,c=4)]
    @test rows(x, :b) == [1, 2]
    @test rows(x, (:b, :c)) == [@NT(b=1,c=3), @NT(b=2,c=4)]
    @test rows(x, (:c, as(-, :b, :x))) == [@NT(c=3, x=-1),@NT(c=4, x=-2)]

    @test keys(x) == [@NT(a=1,b=1), @NT(a=1,b=2)]
    @test keys(x, :a) == [1, 1]
    @test keys(x, (:a, :b, 2)) == [(1,1,1), (1,2,2)]

    @test values(x) == [@NT(c=3), @NT(c=4)]
    @test values(x,1) == [3,4]
    @test values(x,(1,1)) == [(3,3), (4,4)]
    @test values(y) == [3, 4]
    @test values(y,1) == [3,4]
    @test values(y,as(x->compute(sort(x, rev=true)), 1, :x)) == [4, 3]
    @test values(y,(as(-, 1, :x),)) == [@NT(x=-3), @NT(x=-4)]

    @test collect(pairs(x)) == [@NT(a=1,b=1)=>@NT(c=3), @NT(a=1,b=2)=>@NT(c=4)]
    @test collect(pairs(y)) == [@NT(a=1,b=1)=>3, @NT(a=1,b=2)=>4]

    @test names(x) == [:a, :b, :c]
end
