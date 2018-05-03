using JuliaDB
using WeakRefStrings

@testset "WeakRefStrings" begin
    # fix #198, #199
    db = table(WeakRefStrings.StringVector(["A", "B", "A", "B"]), Dagger.distribute(ones(4), 2), names = [:x, :y])
    z = groupreduce(+, db, :x, select = :y)
    @test typeof(z).parameters[1].parameters[1] == String
    @test typeof(z).parameters[2].parameters[1] == String
    @test typeof(z.chunks[1].domain.second).parameters[1].parameters[1] == String
end
