using Base.Test
using JuliaDB
using NamedTuples
using OnlineStats
using DataValues
import DataValues: NA

import JuliaDB: pkeynames, pkeys, excludecols

@testset "table" begin
    t = table([1, 1, 1, 2, 2, 2], [1, 1, 2, 2, 1, 1], [1, 2, 3, 4, 5, 6], names=[:x, :y, :z], pkey=(:x, :y), chunks=2)
    @test groupreduce(+, t, :x, select=:z) == table([1, 2], [6, 15], names=Symbol[:x, :+])
    @test groupreduce(((x, y)->if x isa Int
                        @NT y = x + y
                    else
                        @NT y = x.y + y
                    end), t, :x, select=:z) == table([1, 2], [6, 15], names=Symbol[:x, :y])
    @test groupreduce(:y => (+), t, :x, select=:z) == table([1, 2], [6, 15], names=Symbol[:x, :y])
    x = ndsparse(["a", "b"], [3, 4], chunks=2)
    @test (keytype(x), eltype(x)) == (Tuple{String}, Int64)
    x = ndsparse(@NT(date = Date.(2014:2017)), [4:7;], chunks=2)
    @test x[Date("2015-01-01")] == 5
    @test (keytype(x), eltype(x)) == (Tuple{Date}, Int64)
    x = ndsparse((["a", "b"], [3, 4]), [5, 6], chunks=2)
    @test (keytype(x), eltype(x)) == (Tuple{String,Int64}, Int64)
    @test x["a", 3] == 5
    x = ndsparse((["a", "b"], [3, 4]), ([5, 6], [7.0, 8.0]), chunks=2)
    x = ndsparse(@NT(x = ["a", "a", "b"], y = [3, 4, 4]), @NT(p = [5, 6, 7], q = [8.0, 9.0, 10.0]), chunks=2)
    @test (keytype(x), eltype(x)) == (Tuple{String,Int64}, NamedTuples._NT_p_q{Int64,Float64})
    @test x["a", :] == ndsparse(@NT(y = [3, 4]), Columns(@NT(p = [5, 6], q = [8.0, 9.0])))
    x = ndsparse([1, 2], [3, 4], chunks=2)
    @test pkeynames(x) == (1,)
    x = ndsparse(@NT(t = [0.01, 0.05]), @NT(x = [1, 2], y = [3, 4]), chunks=2)
    manh = map((row->row.x + row.y), x)
    vx = map((row->row.x / row.t), x, select=(:t, :x))
    polar = map((p->@NT(r = hypot(p.x + p.y), θ = atan2(p.y, p.x))), x)
    #@test map(sin, polar, select=:θ) == ndsparse(@NT(t = [0.01, 0.05]), [0.948683, 0.894427])
    a = table([1, 2, 3], [4, 5, 6], chunks=2)
    b = table([1, 2, 3], [4, 5, 6], names=[:x, :y], chunks=2)
    @test table(([1, 2, 3], [4, 5, 6])) == a
    @test table(@NT(x = [1, 2, 3], y = [4, 5, 6])) == b
    @test table(Columns([1, 2, 3], [4, 5, 6])) == a
    @test table(Columns(x=[1, 2, 3], y=[4, 5, 6])) == b
    @test b == table(b)
    b = table([2, 3, 1], [4, 5, 6], names=[:x, :y], pkey=:x, chunks=2)
    b = table([2, 1, 2, 1], [2, 3, 1, 3], [4, 5, 6, 7], names=[:x, :y, :z], pkey=(:x, :y), chunks=2)
    t = table([1, 2], [3, 4], chunks=2)
    @test pkeynames(t) == ()
    t = table([1, 2], [3, 4], pkey=1, chunks=2)
    @test pkeynames(t) == (1,)
    t = table([2, 1], [1, 3], [4, 5], names=[:x, :y, :z], pkey=(1, 2), chunks=2)
    @test pkeys(collect(t)) == Columns(@NT(x = [1, 2], y = [3, 1]))
    @test pkeys(a) == Columns((Base.OneTo(3),))
    a = table(["a", "b"], [3, 4], pkey=1, chunks=2)
    @test pkeys(a) == Columns((String["a", "b"],))
    t = table([2, 1], [1, 3], [4, 5], names=[:x, :y, :z], pkey=(1, 2), chunks=2)
    @test excludecols(t, (:x,)) == (2, 3)
    @test excludecols(t, (2,)) == (1, 3)
    @test excludecols(t, pkeynames(t)) == (3,)
    @test excludecols([1, 2, 3], (1,)) == ()
    @test convert(NextTable, Columns(x=[1, 2], y=[3, 4]), Columns(z=[1, 2]), presorted=true) == table([1, 2], [3, 4], [1, 2], names=Symbol[:x, :y, :z])
    @test colnames([1, 2, 3]) == [1]
    @test colnames(Columns([1, 2, 3], [3, 4, 5])) == [1, 2]
    @test colnames(table([1, 2, 3], [3, 4, 5])) == [1, 2]
    @test colnames(Columns(x=[1, 2, 3], y=[3, 4, 5])) == Symbol[:x, :y]
    @test colnames(table([1, 2, 3], [3, 4, 5], names=[:x, :y])) == Symbol[:x, :y]
    @test colnames(ndsparse(Columns(x=[1, 2, 3]), Columns(y=[3, 4, 5]))) == Symbol[:x, :y]
    @test colnames(ndsparse(Columns(x=[1, 2, 3]), [3, 4, 5])) == Any[:x, 2]
    @test colnames(ndsparse(Columns(x=[1, 2, 3]), [3, 4, 5])) == Any[:x, 2]
    @test colnames(ndsparse(Columns([1, 2, 3], [4, 5, 6]), Columns(x=[6, 7, 8]))) == Any[1, 2, :x]
    @test colnames(ndsparse(Columns(x=[1, 2, 3]), Columns([3, 4, 5], [6, 7, 8]))) == Any[:x, 2, 3]
    t = table([1, 2], [3, 4], names=[:x, :y], chunks=2)
    @test columns(t) == @NT(x = [1, 2], y = [3, 4])
    @test columns(t, :x) == [1, 2]
    @test columns(t, (:x,)) == @NT(x = [1, 2])
    @test columns(t, (:y, :x => (-))) == @NT(y = [3, 4], x = [-1, -2])
    t = table([1, 2], [3, 4], names=[:x, :y], chunks=2)
    @test rows(t) == Columns(@NT(x = [1, 2], y = [3, 4]))
    @test rows(t, :x) == [1, 2]
    @test rows(t, (:x,)) == Columns(@NT(x = [1, 2]))
    @test rows(t, (:y, :x => (-))) == Columns(@NT(y = [3, 4], x = [-1, -2]))
    t = table([1, 2], [3, 4], names=[:x, :y], chunks=2)
    @test setcol(t, 2, [5, 6]) == table([1, 2], [5, 6], names=Symbol[:x, :y])
    @test setcol(t, :x, :x => (x->1 / x)) == table([1.0, 0.5], [3, 4], names=Symbol[:x, :y])
    t = table([0.01, 0.05], [1, 2], [3, 4], names=[:t, :x, :y], pkey=:t, chunks=2)
    t2 = setcol(t, :t, [0.1, 0.05])
    #@test t == t2
    t = table([0.01, 0.05], [2, 1], [3, 4], names=[:t, :x, :y], pkey=:t, chunks=2)
    @test pushcol(t, :z, [1 // 2, 3 // 4]) == table([0.01, 0.05], [2, 1], [3, 4], Rational{Int64}[1//2, 3//4], names=Symbol[:t, :x, :y, :z])
    t = table([0.01, 0.05], [2, 1], [3, 4], names=[:t, :x, :y], pkey=:t, chunks=2)
    @test popcol(t, :x) == table([0.01, 0.05], [3, 4], names=Symbol[:t, :y])
    t = table([0.01, 0.05], [2, 1], [3, 4], names=[:t, :x, :y], pkey=:t, chunks=2)
    @test insertcol(t, 2, :w, [0, 1]) == table([0.01, 0.05], [0, 1], [2, 1], [3, 4], names=Symbol[:t, :w, :x, :y])
    t = table([0.01, 0.05], [2, 1], [3, 4], names=[:t, :x, :y], pkey=:t, chunks=2)
    @test insertcolafter(t, :t, :w, [0, 1]) == table([0.01, 0.05], [0, 1], [2, 1], [3, 4], names=Symbol[:t, :w, :x, :y])
    t = table([0.01, 0.05], [2, 1], [3, 4], names=[:t, :x, :y], pkey=:t, chunks=2)
    @test insertcolbefore(t, :x, :w, [0, 1]) == table([0.01, 0.05], [0, 1], [2, 1], [3, 4], names=Symbol[:t, :w, :x, :y])
    t = table([0.01, 0.05], [2, 1], names=[:t, :x], chunks=2)
    @test renamecol(t, :t, :time) == table([0.01, 0.05], [2, 1], names=Symbol[:time, :x])
    l = table([1, 1, 2, 2], [1, 2, 1, 2], [1, 2, 3, 4], names=[:a, :b, :c], pkey=(:a, :b), chunks=2)
    r = table([0, 1, 1, 3], [1, 1, 2, 2], [1, 2, 3, 4], names=[:a, :b, :d], pkey=(:a, :b), chunks=2)
    @test join(l, r) == table([1, 1], [1, 2], [1, 2], [2, 3], names=Symbol[:a, :b, :c, :d])
    @test join(l, r, how=:left) == table([1, 1, 2, 2], [1, 2, 1, 2], [1, 2, 3, 4], DataValueArray([2, 3, NA, NA]), names=Symbol[:a, :b, :c, :d])
    @test join(l, r, how=:outer) == table([0, 1, 1, 2, 2, 3], [1, 1, 2, 1, 2, 2], DataValueArray([NA, 1, 2, 3, 4, NA]), DataValueArray([1, 2, 3, NA, NA, 4]), names=Symbol[:a, :b, :c, :d])
    @test join(l, r, how=:anti) == table([2, 2], [1, 2], [3, 4], names=Symbol[:a, :b, :c])
    l1 = table([1, 2, 2, 3], [1, 2, 3, 4], names=[:x, :y], chunks=2)
    r1 = table([2, 2, 3, 3], [5, 6, 7, 8], names=[:x, :z], chunks=2)
    @test join(l1, r1, lkey=:x, rkey=:x) == table([2, 2, 2, 2, 3, 3], [2, 2, 3, 3, 4, 4], [5, 6, 5, 6, 7, 8], names=Symbol[:x, :y, :z])
    @test join(l, r, lkey=:a, rkey=:a, lselect=:b, rselect=:d, how=:outer) == table([0, 1, 1, 1, 1, 2, 2, 3], DataValueArray([NA, 1, 1, 2, 2, 1, 2, NA]), DataValueArray([1, 2, 3, 2, 3, NA, NA, 4]), names=Symbol[:a, :b, :d])
    l = table([1, 1, 1, 2], [1, 2, 2, 1], [1, 2, 3, 4], names=[:a, :b, :c], pkey=(:a, :b), chunks=2)
    r = table([0, 1, 1, 2], [1, 2, 2, 1], [1, 2, 3, 4], names=[:a, :b, :d], pkey=(:a, :b), chunks=2)
    #=
    @test groupjoin(l, r) == table([1, 2], [2, 1], [Columns(@NT(c = [2, 2, 3, 3], d = [2, 3, 2, 3])), Columns(@NT(c = [4], d = [4]))], names=Symbol[:a, :b, :groups])
    @test groupjoin(l, r, how=:left) == table([1, 1, 2], [1, 2, 1], [Columns(@NT(c = [], d = [])), Columns(@NT(c = [2, 2, 3, 3], d = [2, 3, 2, 3])), Columns(@NT(c = [4], d = [4]))], names=Symbol[:a, :b, :groups])
    @test groupjoin(l, r, how=:outer) == table([0, 1, 1, 2], [1, 1, 2, 1], [Columns(@NT(c = [], d = [])), Columns(@NT(c = [], d = [])), Columns(@NT(c = [2, 2, 3, 3], d = [2, 3, 2, 3])), Columns(@NT(c = [4], d = [4]))], names=Symbol[:a, :b, :groups])
    @test groupjoin(l, r, lkey=:a, rkey=:a, lselect=:c, rselect=:d, how=:outer) == table([0, 1, 2], [Columns(@NT(c = [], d = [])), Columns(@NT(c = [1, 1, 2, 2, 3, 3], d = [2, 3, 2, 3, 2, 3])), Columns(@NT(c = [4], d = [4]))], names=Symbol[:a, :groups])
    =#
    x = ndsparse((["ko", "ko", "xrx", "xrx"], Date.(["2017-11-11", "2017-11-12", "2017-11-11", "2017-11-12"])), [1, 2, 3, 4], chunks=2)
    y = ndsparse((["ko", "ko", "xrx", "xrx"], Date.(["2017-11-12", "2017-11-13", "2017-11-10", "2017-11-13"])), [5, 6, 7, 8], chunks=2)
    @test asofjoin(x, y) == ndsparse((String["ko", "ko", "xrx", "xrx"], Date.(["2017-11-11", "2017-11-12", "2017-11-11", "2017-11-12"])), [1, 5, 7, 7])
    a = table([1, 3, 5], [1, 2, 3], names=[:x, :y], pkey=:x, chunks=2)
    b = table([2, 3, 4], [1, 2, 3], names=[:x, :y], pkey=:x, chunks=2)
    @test merge(a, b) == table([1, 2, 3, 3, 4, 5], [1, 1, 2, 2, 3, 3], names=Symbol[:x, :y])
    a = ndsparse([1, 3, 5], [1, 2, 3], chunks=2)
    b = ndsparse([2, 3, 4], [1, 2, 3], chunks=2)
    @test merge(a, b) == ndsparse(([1, 2, 3, 4, 5],), [1, 1, 2, 3, 3])
    @test merge(a, b, agg=+) == ndsparse(([1, 2, 3, 4, 5],), [1, 1, 4, 3, 3])
    a = ndsparse(([1, 1, 2, 2], [1, 2, 1, 2]), [1, 2, 3, 4], chunks=2)
    b = ndsparse([1, 2], [1 / 1, 1 / 2], chunks=2)
    @test broadcast(*, a, b) == ndsparse(([1, 1, 2, 2], [1, 2, 1, 2]), [1.0, 2.0, 1.5, 2.0])
    @test a .* b == ndsparse(([1, 1, 2, 2], [1, 2, 1, 2]), [1.0, 2.0, 1.5, 2.0])
    @test broadcast(*, a, b, dimmap=(0, 1)) == ndsparse(([1, 1, 2, 2], [1, 2, 1, 2]), [1.0, 1.0, 3.0, 2.0])
    t = table([0.1, 0.5, 0.75], [0, 1, 2], names=[:t, :x], chunks=2)
    @test reduce(+, t, select=:t) == 1.35
    @test reduce(((a, b)->@NT(t = a.t + b.t, x = a.x + b.x)), t) == @NT(t = 1.35, x = 3)
    @test using OnlineStats == nothing
    @test value(reduce(Mean(), t, select=:t)) == (0.45,)
    y = reduce((min, max), t, select=:x)
    @test y.max == 2
    @test y.min == 0
    y = reduce(@NT(sum = (+), prod = (*)), t, select=:x)
    y = reduce((Mean(), Variance()), t, select=:t)
    @test value(y.Mean) == (0.45,)
    @test value(y.Variance) == (0.10749999999999998,)
    @test reduce(@NT(xsum = (:x => (+)), negtsum = ((:t => (-)) => (+))), t) == @NT(xsum = 3, negtsum = -1.35)
    t = table([1, 1, 1, 2, 2, 2], [1, 1, 2, 2, 1, 1], [1, 2, 3, 4, 5, 6], names=[:x, :y, :z], chunks=2)
    @test groupreduce(+, t, :x, select=:z) == table([1, 2], [6, 15], names=Symbol[:x, :+])
    @test groupreduce(+, t, (:x, :y), select=:z) == table([1, 1, 2, 2], [1, 2, 1, 2], [3, 3, 11, 4], names=Symbol[:x, :y, :+])
    @test groupreduce((+, min, max), t, (:x, :y), select=:z) == table([1, 1, 2, 2], [1, 2, 1, 2], [3, 3, 11, 4], [1, 3, 5, 4], [2, 3, 6, 4], names=Symbol[:x, :y, :+, :min, :max])
    @test groupreduce(@NT(zsum = (+), zmin = min, zmax = max), t, (:x, :y), select=:z) == table([1, 1, 2, 2], [1, 2, 1, 2], [3, 3, 11, 4], [1, 3, 5, 4], [2, 3, 6, 4], names=Symbol[:x, :y, :zsum, :zmin, :zmax])
    @test groupreduce(@NT(xsum = (:x => (+)), negysum = ((:y => (-)) => (+))), t, :x) == table([1, 2], [3, 6], [-4, -4], names=Symbol[:x, :xsum, :negysum])
    t = table([1, 1, 1, 2, 2, 2], [1, 1, 2, 2, 1, 1], [1, 2, 3, 4, 5, 6], names=[:x, :y, :z], chunks=2)
    @test groupby(mean, t, :x, select=:z) == table([1, 2], [2.0, 5.0], names=Symbol[:x, :mean])
    @test groupby(identity, t, (:x, :y), select=:z) == table([1, 1, 2, 2], [1, 2, 1, 2], [[1, 2], [3], [5, 6], [4]], names=Symbol[:x, :y, :identity])
    @test groupby(mean, t, (:x, :y), select=:z) == table([1, 1, 2, 2], [1, 2, 1, 2], [1.5, 3.0, 5.5, 4.0], names=Symbol[:x, :y, :mean])
    #@test groupby((mean, std, var), t, :y, select=:z) == table([1, 2], [3.5, 3.5], [2.38048, 0.707107], [5.66667, 0.5], names=Symbol[:y, :mean, :std, :var])
    @test groupby(@NT(q25 = (z->quantile(z, 0.25)), q50 = median, q75 = (z->quantile(z, 0.75))), t, :y, select=:z) == table([1, 2], [1.75, 3.25], [3.5, 3.5], [5.25, 3.75], names=Symbol[:y, :q25, :q50, :q75])
    #@test groupby(@NT(xmean = (:z => mean), ystd = ((:y => (-)) => std)), t, :x) == table([1, 2], [2.0, 5.0], [0.57735, 0.57735], names=Symbol[:x, :xmean, :ystd])
    x = ndsparse(@NT(x = [1, 1, 1, 2, 2, 2], y = [1, 2, 2, 1, 2, 2], z = [1, 1, 2, 1, 1, 2]), [1, 2, 3, 4, 5, 6], chunks=2)
    @test reducedim(+, x, 1) == ndsparse(@NT(y = [1, 2, 2], z = [1, 1, 2]), [5, 7, 9])
    @test reducedim(+, x, (1, 3)) == ndsparse(@NT(y = [1, 2]), [5, 16])
    tbl = table([0.01, 0.05], [2, 1], [3, 4], names=[:t, :x, :y], pkey=:t, chunks=2)
    @test select(tbl, 2) == [2, 1]
    @test select(tbl, :t) == [0.01, 0.05]
    @test select(tbl, :t => (t->1 / t)) == [100.0, 20.0]
    @test select(tbl, [3, 4]) == [3, 4]
    @test select(tbl, (2, 1)) == table([2, 1], [0.01, 0.05], names=Symbol[:x, :t])
    vx = select(tbl, (:x, :t) => (p->p.x / p.t))
    @test select(tbl, (:x, :t => (-))) == table([1, 2], [-0.05, -0.01], names=Symbol[:x, :t])
    @test select(tbl, (:x, :t, [3, 4])) == table([2, 1], [0.01, 0.05], [3, 4], names=[1, 2, 3])
    @test select(tbl, (:x, :t, :z => [3, 4])) == table([2, 1], [0.01, 0.05], [3, 4], names=Symbol[:x, :t, :z])
    @test select(tbl, (:x, :t, :minust => (:t => (-)))) == table([2, 1], [0.01, 0.05], [-0.01, -0.05], names=Symbol[:x, :t, :minust])
    @test select(tbl, (:x, :t, :vx => ((:x, :t) => (p->p.x / p.t)))) == table([2, 1], [0.01, 0.05], [200.0, 20.0], names=Symbol[:x, :t, :vx])
    t = table([2, 1], [1, 3], [4, 5], names=[:x, :y, :z], pkey=(1, 2), chunks=2)
    @test reindex(t, (:y, :z)) == table([1, 3], [4, 5], [2, 1], names=Symbol[:y, :z, :x])
    @test pkeynames(t) == (:x, :y)
    #@test reindex(t, (:w => [4, 5], :z)) == table([4, 5], [5, 4], [1, 2], [3, 1], names=Symbol[:w, :z, :x, :y])
    @test pkeynames(t) == (:x, :y)
    t = table([0.01, 0.05], [1, 2], [3, 4], names=[:t, :x, :y], chunks=2)
    manh = map((row->row.x + row.y), t)
    polar = map((p->@NT(r = hypot(p.x + p.y), θ = atan2(p.y, p.x))), t)
    vx = map((row->row.x / row.t), t, select=(:t, :x))
    #@test collect(map(sin, polar, select=:θ)) == [0.948683, 0.894427]
    t = table([0.1, 0.5, NA, 0.7], [2, NA, 4, 5], [NA, 6, NA, 7], names=[:t, :x, :y], chunks=2)
    @test dropna(t) == table([0.7], [5], [7], names=Symbol[:t, :x, :y])
    @test dropna(t, :y) == table(DataValues.DataValue{Float64}[0.5, 0.7], DataValues.DataValue{Int64}[NA, 5], [6, 7], names=Symbol[:t, :x, :y])
    t1 = dropna(t, (:t, :x))
    @test typeof(column(dropna(t, :x), :x)) <: Dagger.DArray{Int64,1}
    t = table(["a", "b", "c"], [0.01, 0.05, 0.07], [2, 1, 0], names=[:n, :t, :x], chunks=2)
    @test filter((p->p.x / p.t < 100), t) == table(String["b", "c"], [0.05, 0.07], [1, 0], names=Symbol[:n, :t, :x])
    x = ndsparse(@NT(n = ["a", "b", "c"], t = [0.01, 0.05, 0.07]), [2, 1, 0], chunks=2)
    @test filter((y->y < 2), x) == ndsparse(@NT(n = String["b", "c"], t = [0.05, 0.07]), [1, 0])
    @test filter(iseven, t, select=:x) == table(String["a", "c"], [0.01, 0.07], [2, 0], names=Symbol[:n, :t, :x])
    @test filter((p->p.x / p.t < 100), t, select=(:x, :t)) == table(String["b", "c"], [0.05, 0.07], [1, 0], names=Symbol[:n, :t, :x])
    @test filter((p->p[2] / p[1] < 100), x, select=(:t, 3)) == ndsparse(@NT(n = String["b", "c"], t = [0.05, 0.07]), [1, 0])
    @test filter((:x => iseven, :t => (a->a > 0.01)), t) == table(String["c"], [0.07], [0], names=Symbol[:n, :t, :x])
    @test filter((3 => iseven, :t => (a->a > 0.01)), x) == ndsparse(@NT(n = String["c"], t = [0.07]), [0])
    b = table(["a","a","b","b"], [1,3,5,7], [2,2,2,2], names = [:x, :y, :z], pkey = :x, chunks = 2)
    @test summarize(mean, b) == table(["a","b"], [2.0,6.0], [2.0,2.0], names = [:x, :y_mean, :z_mean], pkey = :x)
end
