@testset "join" begin
    t1 = NDSparse(Columns(([1,1,2,2], [1,2,1,2])), [1,2,3,4])
    t2 = NDSparse(Columns(([0,2,2,3], [1,1,2,2])), [1,2,3,4])

    j1 = innerjoin(t1,t2)
    j2 = innerjoin(+, t1,t2)

    lj1 = leftjoin(t1,t2)
    lj2 = leftjoin(+, t1,t2)

    mj1 = merge(t1,t2)
    mj2 = merge(t2,t1)
    nn = Ref{Int}()
    nn2 = Ref{Int}()
    for n=1:5
        nn[] = n
        d1 = distribute(t1, n)
        for n2 = 1:5
            nn2[] = n2
            d2 = distribute(t2, n2)
            @test isequal(collect(innerjoin(d1, d2)), j1)
            @test isequal(collect(innerjoin(+, d1, d2)), j2)

            @test isequal(collect(leftjoin(d1, d2)), lj1)
            @test isequal(collect(leftjoin(+, d1, d2)), lj2)

            @test isequal(collect(merge(d1, d2)), mj1)
            @test isequal(collect(merge(d2, d1)), mj2)
        end
    end

    t1 = NDSparse([:msft,:ibm,:ge], [1,3,4], [100,200,150])
    t2 = NDSparse([:ibm,:msft,:aapl,:ibm], [0,0,0,2], [100,99,101,98])

    aj = asofjoin(t1,t2)

    for n=1:5
        d1 = distribute(t1, n)
        for n2 = 1:5
            d2 = distribute(t2, n2)
            @test collect(asofjoin(d1, d2)) == aj
        end
    end
end

@testset "broadcast" begin
    t1 = NDSparse(Columns(([1,2,3,4],[1,1,2,2])), [5,6,7,8])
    t2 = NDSparse(Columns(([0,1,2,4],)),[0,10,100,10000])

    for n=1:4
        for m=1:4
            d1 = distribute(t1, n)
            d2 = distribute(t2, m)

            @test collect(d1 .+ d2) == t1 .+ t2
            @test collect(broadcast(+, d1, d2, dimmap=(0,1))) == broadcast(+, t1, t2, dimmap=(0,1))
        end
    end
end

@testset "join with missing" begin 
    y = rand(10)
    z = rand(10)

    # DIndexedTable
    t  = table((x=1:10,   y=y), pkey=:x, chunks=2)
    t2 = table((x=1:2:20, z=z), pkey=:x, chunks=2)

    # DNDSparse
    nd = ndsparse((x=1:10,), (y=y,), chunks=2)
    nd2 = ndsparse((x=1:2:20,), (z=z,), chunks=2)

    @testset "how = :left" begin
        # Missing
        z_left = Union{Float64,Missing}[missing for i in 1:10]
        z_left[1:2:9] = z[1:5]
        t_left = table((x = 1:10, y = y, z = z_left))
        nd_left = ndsparse((x=1:10,), (y=y, z=z_left))
        @test isequal(join(t, t2; how=:left), t_left)
        @test isequal(join(nd, nd2; how=:left), nd_left)

        # DataValue
        z_left2 = [DataValue{Float64}() for i in 1:10]
        z_left2[1:2:9] = z[1:5]
        t_left2 = table((x=1:10, y = y, z = z_left2))
        nd_left2 = ndsparse((x=1:10,), (y=y, z=z_left2))
        @test isequal(join(t, t2, how=:left, missingtype=DataValue), t_left2)
        @test isequal(join(nd, nd2, how=:left, missingtype=DataValue), nd_left2)
    end
end
