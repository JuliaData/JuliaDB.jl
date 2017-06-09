using JuliaDB
using IndexedTables
using Base.Test

@testset "join" begin
    t1 = IndexedTable(Columns([1,1,2,2], [1,2,1,2]), [1,2,3,4])
    t2 = IndexedTable(Columns([0,2,2,3], [1,1,2,2]), [1,2,3,4])

    j1 = innerjoin(t1,t2)
    j2 = innerjoin(t1,t2,+)

    lj1 = leftjoin(t1,t2)
    lj2 = leftjoin(t1,t2,+)

    mj1 = merge(t1,t2)
    mj2 = merge(t2,t1)

    for n=1:5
        d1 = distribute(t1, n)
        for n2 = 1:5
            d2 = distribute(t2, n2)
            @test collect(innerjoin(d1, d2)) == j1
            @test collect(innerjoin(d1, d2, +)) == j2

            @test collect(leftjoin(d1, d2)) == lj1
            @test collect(leftjoin(d1, d2, +)) == lj2

            @test collect(merge(d1, d2)) == mj1
            @test collect(merge(d2, d1)) == mj2
        end
    end

    t1 = IndexedTable([:msft,:ibm,:ge], [1,3,4], [100,200,150])
    t2 = IndexedTable([:ibm,:msft,:aapl,:ibm], [0,0,0,2], [100,99,101,98])

    aj = asofjoin(t1,t2)

    for n=1:5
        d1 = distribute(t1, n)
        for n2 = 1:5
            d2 = distribute(t2, n2)
            @test collect(asofjoin(d1, d2)) == aj
        end
    end
end
