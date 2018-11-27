using Test
using JuliaDB
using PooledArrays
using DataValues
using MemPool
using Random
using Serialization
using Dagger

function roundtrip(x, eq=(==), io=IOBuffer())
    mmwrite(Serializer(io), x)
    @test eq(deserialize(seekstart(io)), x)
end

@testset "PooledArray/DataValueArray" begin
    roundtrip(PooledArray([randstring(rand(1:10)) for i=4]))
    roundtrip(DataValueArray(rand(10), rand(Bool,10)), isequal)
end

@testset "Columns" begin
    roundtrip(Columns([1,2], ["x","y"]))
    roundtrip(Columns(x=[1,2], y=["x","y"]))
end

@testset "ndsparse" begin
    ndsparse(Columns([1,2], ["x","y"]),
             Columns(x=[1,2], y=["x","y"])) |> roundtrip
end

@testset "table" begin
    table([1,2], ["x","y"]) |> roundtrip
    table(Columns(x=[1,2], y=["x","y"])) |> roundtrip
end


path = joinpath(dirname(@__FILE__), "..","test", "sample")
files = glob("*.csv", path)
const spdata_dist = loadndsparse(files, type_detect_rows=4,
                              indexcols=1:2, usecache=false, chunks=2)
const spdata_dist_path = loadndsparse(path, type_detect_rows=4,
                              indexcols=1:2, usecache=false, chunks=2)

loadndsparse(files[1:2], chunks=4)

_readstr(f) = open(f) do fh
    readline(fh)
    readstring(fh)
end
const spdata = loadndsparse(files;
                            distributed=false,
                            header_exists=true,
                            indexcols=1:2)
files = glob("*.csv", "sample")
shuffle_files = shuffle(files)
const spdata_unordered = loadndsparse(shuffle_files;
                                      distributed=false,
                                      indexcols=[])

ingest_output = tempname()
spdata_ingest = loadndsparse(files, output=ingest_output, indexcols=1:2, chunks=2)
ingest_output_unordered = tempname()
# note: this will result in a different table if files[3:end] is ingested first
spdata_ingest_unordered = loadndsparse(shuffle_files, output=ingest_output_unordered,
                                 indexcols=[], chunks=2)
# spdata_ingest_unordered = loadndsparse(shuffle_files[4:end], output=ingest_output_unordered,
#                                        append=true, indexcols=[])
# this should also test appending new files

import Dagger: Chunk
@testset "Load" begin
    @test loadtable("missingcols/t1.csv") == table([0,0,0], [1,2,3], names=[:a,:x])
    cache = joinpath(JuliaDB.JULIADB_DIR, JuliaDB.JULIADB_FILECACHE)
    if isfile(cache)
        rm(cache)
    end
    missingcoltbl = loadndsparse(joinpath(@__DIR__, "missingcols"), datacols=[:a, :x, :y], usecache=false, chunks=2)
    @test eltype(missingcoltbl) == NamedTuple{(:a,:x,:y),Tuple{Int, DataValue{Int}, DataValue{Float64}}}

    @test collect(loadtable(shuffle_files,chunks=2)) == table(spdata_unordered.data)
    # file name as a column:
    @test unique(keys(loadndsparse(path, indexcols=[:year, :date, :ticker],filenamecol=:year, usecache=false, chunks=2), :year)|> collect) == string.(2010:2015)
    @test unique(keys(loadndsparse(path, indexcols=[:year, :date, :ticker],filenamecol=:year=>(x->x[3:4])∘basename, usecache=false, chunks=2), :year)|> collect) == string.(10:15)
    @test collect(spdata_dist) == spdata
    @test collect(spdata_dist_path) == spdata
    @test collect(spdata_ingest) == spdata
    @test collect(load(ingest_output)) == spdata
    @test collect(load(ingest_output_unordered)) == spdata_unordered
    @test issorted(collect(keys(load(ingest_output_unordered), 1)))
    c = first(load(ingest_output).chunks)
    #@test isa(c.handle, FileRef)
    #@test collect(dt[["blah"], :,:]) == spdata
    dt = loadndsparse(files, indexcols=[("date", "dummy"), ("dummy", "ticker")], usecache=false, chunks=2)
    nds=collect(dt)
    @test haskey(nds.index.columns, :date)
    @test haskey(nds.index.columns, :dummy)
    @test !haskey(nds.index.columns, :ticker)
    @test length(nds.index.columns) == 2
    @test keys(nds.data.columns) == (:open, :high, :low, :close, :volume)
    @test length(nds.data.columns) == 5

    dt = loadndsparse(shuffle_files, usecache=false, chunks=2)
    @test collect(dt) == spdata_unordered
    @test issorted(collect(keys(dt, 1)))
    # reuses csv read cache:
    dt = loadndsparse(shuffle_files, indexcols=[], chunks=4, usecache=false)
    @test collect(dt) == spdata_unordered
    dt = loadndsparse(shuffle_files, indexcols=[], chunks=4) # cache test
    @test collect(dt) == spdata_unordered

    # test specifying column names
    dt = loadndsparse(files[1:2], indexcols=[:a,:b], colnames=[:a,:b,:c,:d,:e,:f,:g], usecache=false, header_exists=false, chunks=2)
    nds = collect(dt)
    @test haskey(nds.index.columns, :a)
    @test haskey(nds.index.columns, :b)
    @test keys(nds.data.columns) == (:c,:d,:e,:f,:g)
end

@testset "save" begin
    t = NDSparse([1,2,3,4], [1,2,3,4])
    n = tempname()
    x = JuliaDB.save(distribute(t, 4), n)
    t1 = load(n)
    @test collect(t1) == collect(x)
    @test !any(c->isempty(Dagger.affinity(c.handle)), t1.chunks)
    rm(n, recursive=true)

    t = table([1,2,3,4], [1,2,3,4], chunks=2)
    n = tempname()
    x = JuliaDB.save(t, n)
    t1 = load(n)
    @test collect(t1) == collect(t)
    @test !any(c->isempty(Dagger.affinity(c.handle)), t1.chunks)
    rm(n, recursive=true)
end

rm(ingest_output, recursive=true)
rm(ingest_output_unordered, recursive=true)

@testset "load and save with missings" begin
    nm1, nm2 = tempname(), tempname()
    # Create a csv with one missing value and a column with all missings
    open(nm1, "w") do io
        write(io, "A,B,C\n1,1,NA\n1,NA,NA\n")
    end

    t1 = loadtable(nm1, delim = ',')
    save(t1, nm2)
    t2 = load(nm2)

    @test all([all(collect(c1 .=== c2)) for (c1, c2) in zip(columns(t1), columns(t2))])

    # Clean up
    rm(nm1, recursive=true)
    rm(nm2, recursive=true)
end
