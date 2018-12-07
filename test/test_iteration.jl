@testset "iteration" begin
    x = distribute(NDSparse(Columns(a=[1,1], b=[1,2]), Columns(c=[3,4])), 2)
    y = distribute(NDSparse(Columns(a=[1,1], b=[1,2]), [3,4]), 2)

    @test column(x, :a) == [1,1]
    @test column(x, :b) == [1,2]
    @test column(x, :c) == [3,4]
    @test column(x, 3) == [3,4]
    @test column(y, 3) == [3,4]

    @test columns(x) == (a=[1,1], b=[1,2], c=[3,4])
    @test columns(x, (:a,:c)) == (a=[1,1], c=[3,4])
    @test columns(y, (1, 3)) == ([1,1], [3,4])

    @test rows(x) == [(a=1,b=1,c=3), (a=1,b=2,c=4)]
    @test rows(x, :b) == [1, 2]
    @test rows(x, (:b, :c)) == [(b=1,c=3), (b=2,c=4)]
    #@test rows(x, (:c, as(-, :b, :x))) == [(c=3, x=-1),(c=4, x=-2)]

    @test keys(x) == [(a=1,b=1), (a=1,b=2)]
    @test keys(x, :a) == [1, 1]
    #@test keys(x, (:a, :b, 2)) == [(1,1,1), (1,2,2)]

    @test values(x) == [(c=3,), (c=4,)]
    @test values(x,1) == [3,4]
    #@test values(x,(1,1)) == [(3,3), (4,4)]
    @test values(y) == [3, 4]
    @test values(y,1) == [3,4]
    #@test values(y,as(x->compute(sort(x, rev=true)), 1, :x)) == [4, 3]
    #@test values(y,(as(-, 1, :x),)) == [(x=-3,), (x=-4,)]

    @test collect(pairs(x)) == [(a=1,b=1)=>(c=3,), (a=1,b=2)=>(c=4,)]
    @test collect(pairs(y)) == [(a=1,b=1)=>3, (a=1,b=2)=>4]

    x = ndsparse(([1,2], [3,4]), (x=[0,1],), chunks=2)
    @test ndsparse(keys(x), pushcol(values(x), :y, [1,2])) == ndsparse(([1,2], [3,4]), (x=[0,1], y=[1,2]))
end
