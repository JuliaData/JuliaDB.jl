@testset "online stats" begin
global dt
    t = NDSparse(Columns(([1,1,1,2,2,2,3,3,3], [1,2,3,1,2,3,1,2,3])), [0.5,1,1.5,1,2,3,2,3,4])
    dt = distribute(t,[3,2,4])
    @test value(reduce(Mean(), t))[1] ≈ 2
    @test value(reduce(Mean(), dt))[1] ≈ 2

    means = groupreduce(Series(Mean()), dt, 1)
    @test collect(values(map(x->x.stats[1].μ, means))) == [1.0, 2.0, 3.0]
   # @test length(means.chunks) == 2
   # @test Series((keys(dt), values(dt)), LinReg(2)).stats[1].β |> string == string([0.615385, 0.448718])

   # regs = aggregate_stats(Series(LinReg(2)), keys(dt, 1), keys(dt), values(dt))
   # @test isapprox(values(map(x->coef(x.stats[1]), regs)) |> collect, [[0, 0.5], [0.0, 1.0], [0.3333333, 1.0]], rtol=10e-5) |> all
end
