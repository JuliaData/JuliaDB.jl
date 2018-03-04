import Dagger: debug_compute, get_logs!, LocalEventLog

export start_tracking_time, stop_tracking_time, tracktime, fetch_timings!

function time_table(log; profile=false)

    idx = Columns(proc=map(x->x.pid, getfields(log, :timeline)),
                  event_type=getfields(log, :category),
                  event_id=getfields(log, :id))

    if profile
        data = Columns(start=getfields(log, :start),   finish=getfields(log, :finish),
                       gc_diff=getfields(log, :gc_diff), profile=getfields(log, :profiler_samples))
    else
        data = Columns(start=getfields(log, :start),   finish=getfields(log, :finish),
                       gc_diff=getfields(log, :gc_diff))
    end

    NDSparse(idx, data)
end

function add_gc_diff(x,y)
    Base.GC_Diff(
        x.allocd     + y.allocd,
        x.malloc     + y.malloc,
        x.realloc    + y.realloc,
        x.poolalloc  + y.poolalloc,
        x.bigalloc   + y.bigalloc,
        x.freecall   + y.freecall,
        x.total_time + y.total_time,
        x.pause      + y.pause,
        x.full_sweep + y.full_sweep
    )
end

function aggregate_profile(xs)
    treereduce(Dagger.mix_samples, xs)
end

function aggregate_events(xs)
    sort!(xs, by=x->x.start)
    gc_diff = reduce(add_gc_diff, map(x -> x.gc_diff, xs))
    time_spent = sum(map(x -> x.finish - x.start, xs))
    if isdefined(xs[1], :profile)
        time_spent, gc_diff, aggregate_profile(map(x->x.profile, xs))
    end
    time_spent, gc_diff
end

function show_timings(t; maxdepth=5)
    # first aggregate by type of event
    t1 = reducedim_vec(aggregate_events, t, [:proc, :event_id])

    foreach(t1.index, t1.data) do i, x
        time_spent, gc_diff = x
        print(string(i[1]),": ")
        Base.time_print(time_spent, gc_diff.allocd, gc_diff.total_time, Base.gc_alloc_count(gc_diff))
    end

    t2 = reducedim_vec(aggregate_events, t, :event_id)
    println("Breakdown:")
    println(map(x->first(x)/1e9, t2))
    if isdefined(t.data.columns, :profile)
        p = aggregate_profile(t.data.columns.profile)
        if !isempty(p.samples)
            println("\nProfile output:")
            Profile.print(p.samples, p.lineinfo, maxdepth=maxdepth)
        end
    end
end

function getfields(log, fieldname)
    map(x->getfield(x, fieldname), log)
end

function start_tracking_time(;profile=false)
    ctx = get_context()
    dbgctx = Context(procs(ctx), LocalEventLog(), profile)
    compute_context[] = dbgctx
end

function stop_tracking_time()
    compute_context[] = nothing
end

"""
`tracktime(f)`

Track the time spent on different processes in different
categories in running `f`.
"""
function tracktime(f; profile=false, maxdepth=5)
    start_tracking_time(profile=profile)
    res = f()
    ctx = compute_context[]
    stop_tracking_time()
    t = fetch_timings!(ctx, profile=profile)
    show_timings(t, maxdepth=maxdepth)
    t, res
end

function fetch_timings!(ctx=get_context(); profile=true)
    time_table(Dagger.get_logs!(ctx.log_sink), profile=profile)
end
