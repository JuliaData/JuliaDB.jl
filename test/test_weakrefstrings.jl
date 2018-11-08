using JuliaDB
using WeakRefStrings
using Dagger

@testset "WeakRefStrings" begin
    # fix #198, #199
    db = table(WeakRefStrings.StringVector(["A", "B", "A", "B"]), Dagger.distribute(ones(4), 2), names = [:x, :y])
    z = groupreduce(+, db, :x, select = :y)
    @test fieldtype(typeof(z).parameters[1], 1) == String
    @test fieldtype(typeof(z).parameters[2], 1) == String
    @test fieldtype(typeof(z.chunks[1].domain.second).parameters[1], 1) == String
end
