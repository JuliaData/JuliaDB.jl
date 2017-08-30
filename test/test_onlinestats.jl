using JuliaDB
using OnlineStats
using Base.Test

@testset "online stats" begin
    t = IndexedTable(Columns([1,1,1,2,2,2,3,3,3], [1,2,3,1,2,3,1,2,3]), [0.5,1,1.5,1,2,3,2,3,4])
    dt = distribute(t,[3,2,4])
    @test Series(t, Mean()).stats.μ == 2.0
    @test Series(dt, Mean()).stats.μ == 2.0
    means = aggregate_stats(Series(Mean()), select(dt,1))
    @test values(map(x->x.stats.μ, means)) == [1.0, 2.0, 3.0]
    @test length(means.chunks) == 2
    @test Series(keys(dt), values(dt), LinReg(2)).stats.β |> string == string([0.615385, 0.448718])

    regs = aggregate_stats(Series(LinReg(2)), keys(dt, 1), keys(dt), values(dt))
    @test isapprox(values(map(x->coef(x.stats), regs)) |> collect, [[0, 0.5], [0.0, 1.0], [0.3333333, 1.0]], rtol=10e-5) |> all
end
