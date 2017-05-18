@testset "rechunk" begin
    t = IndexedTable([1:10;], ones(10), ones(10))
    d = distribute(t, 2)
    d2 = compute(rechunk(d, [2,3,4,1]))
    @test get.(map(JuliaDB.nrows, d2.subdomains)) == [2,3,4,1]
end
