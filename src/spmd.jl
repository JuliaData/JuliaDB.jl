module SPMD

let DID::Int = 1 
    global next_did
    next_did() = (id = DID; DID += 1; (myid(), id))
end
            
import Dagger: gather
import Base: close, recvfrom
export sendto, recvfrom, recvfrom_any, barrier, bcast, scatter, gather
export context_local_storage, context, spmd, close


type WorkerDataChannel
    pid::Int
    rc::Nullable{RemoteChannel}
    lock::ReentrantLock

    WorkerDataChannel(pid) = new(pid, Nullable{RemoteChannel}(), ReentrantLock())
end

type SPMDContext
    id::Tuple
    chnl::Channel
    store::Dict{Any,Any}
    pids::Array
    release::Bool

    function SPMDContext(id)
        ctxt = new(id, Channel(typemax(Int)), Dict{Any,Any}(), [], false)
        finalizer(ctxt, finalize_ctxt)
        ctxt
    end
end

function finalize_ctxt(ctxt::SPMDContext)
    ctxt.release && close(ctxt)
end

function context_local_storage()
    ctxt = get_ctxt_from_id(task_local_storage(:SPMD_CTXT))
    ctxt.store
end

function context(pids=procs())
    global map_ctxts
    ctxt = SPMDContext(next_did())
    ctxt.pids = pids
    ctxt.release = true
    ctxt
end

# Every worker is associated with its own RemoteChannel
const map_worker_channels = Dict{Int, WorkerDataChannel}()

# mapping between a context id and context object
const map_ctxts = Dict{Tuple, SPMDContext}()

# Multiple SPMD blocks can be executed concurrently,
# each in its own context. Messages are still sent as part of the
# same remote channels associated with each worker. They are
# read from the remote channel into local channels each associated
# with a different run of `spmd`.

function get_dc(wc::WorkerDataChannel)
    lock(wc.lock)
    try
        if isnull(wc.rc)
            if wc.pid == myid()
                myrc = RemoteChannel(()->Channel(typemax(Int)))
                wc.rc = Nullable{RemoteChannel}(myrc)

                # start a task to transfer incoming messages into local
                # channels based on the execution context
                @schedule begin
                    while true
                        msg = take!(myrc)
                        ctxt_id = msg[1] # First element of the message tuple is the context id.
                        ctxt = get_ctxt_from_id(ctxt_id)
                        put!(ctxt.chnl, msg[2:end]) # stripping the context_id
                    end
                end
            else
                wc.rc = Nullable{RemoteChannel}(remotecall_fetch(()->get_remote_dc(myid()), wc.pid))
            end
        end
    finally
        unlock(wc.lock)
    end
    return get(wc.rc)
end

function get_ctxt_from_id(ctxt_id)
    global map_ctxts
    ctxt = get(map_ctxts, ctxt_id, nothing)
    if ctxt == nothing
        ctxt = SPMDContext(ctxt_id)
        map_ctxts[ctxt_id] = ctxt
    end
    return ctxt
end


# Since modules may be loaded in any order on the workers,
# and workers may be dynamically added, pull in the remote channel
# handles when accessed for the first time.
function get_remote_dc(pid)
    global map_worker_channels
    if !haskey(map_worker_channels, pid)
        map_worker_channels[pid] = WorkerDataChannel(pid)
    end

    return get_dc(map_worker_channels[pid])
end

function send_msg(to, typ, data, tag)
    ctxt_id = task_local_storage(:SPMD_CTXT)
    @async begin
        dc = get_remote_dc(to)
        put!(dc, (ctxt_id, typ, myid(), data, tag))
#        println("Sent to ", dc)
    end
end

function get_msg(typ_check, from_check=false, tag_check=nothing)
    ctxt_id = task_local_storage(:SPMD_CTXT)
    chnl = get_ctxt_from_id(ctxt_id).chnl

    unexpected_msgs=[]
    while true
        typ, from, data, tag = take!(chnl)

        if (from_check != false && from_check != from) || (typ != typ_check) || (tag != tag_check)
            push!(unexpected_msgs, (typ, from, data, tag))
#            println("Unexpected in get_msg ", unexpected_msgs, " looking for ", typ_check, " ", from_check, " ", tag_check)
        else
            # put all the messages we read (but not expected) back to the local channel
            foreach(x->put!(chnl, x), unexpected_msgs)
            return (from, data)
        end
    end
end

function sendto(pid::Int, data::Any; tag=nothing)
    send_msg(pid, :sendto, data, tag)
end

function recvfrom(pid::Int; tag=nothing)
    _, data = get_msg(:sendto, pid, tag)
    return data
end

function recvfrom_any(; tag=nothing)
    from, data = get_msg(:sendto, false, tag)
    return (from,data)
end

function barrier(;pids=procs(), tag=nothing)
    # send a message to everyone
    for p in sort(pids)
        send_msg(p, :barrier, nothing, tag)
    end
    # make sure we recv a message from everyone
    pending=deepcopy(pids)
    unexpected_msgs=[]

    while length(pending) > 0
        from, _ = get_msg(:barrier, false, tag)
        if from in pending
            filter!(x->x!=from, pending)
        else
            # handle case of 2 (or more) consecutive barrier calls.
            push!(unexpected_msgs, (:barrier, from, nothing, tag))
#            println("Unexpected ", from)
        end
#        length(pending) == 1 && println("Waiting for ", pending)
    end

    ctxt_id = task_local_storage(:SPMD_CTXT)
    chnl = get_ctxt_from_id(ctxt_id).chnl
    foreach(x->put!(chnl, x), unexpected_msgs)
    return nothing
end

function bcast(data::Any, pid::Int; tag=nothing, pids=procs())
    if myid() == pid
        for p in filter(x->x!=pid, sort(pids))
            send_msg(p, :bcast, data, tag)
        end
        return data
    else
        from, data = get_msg(:bcast, pid, tag)
        return data
    end
end

function scatter(x, pid::Int; tag=nothing, pids=procs())
    if myid() == pid
        @assert rem(length(x), length(pids)) == 0
        cnt = div(length(x), length(pids))
        for (i,p) in enumerate(sort(pids))
            p == pid && continue
            send_msg(p, :scatter, x[cnt*(i-1)+1:cnt*i], tag)
        end
        myidx = findfirst(sort(pids), pid)
        return x[cnt*(myidx-1)+1:cnt*myidx]
    else
        _, data = get_msg(:scatter, pid, tag)
        return data
    end
end

function gather(x, pid::Int; tag=nothing, pids=procs())
    if myid() == pid
        gathered_data = Array{Any}(length(pids))
        myidx = findfirst(sort(pids), pid)
        gathered_data[myidx] = x
        n = length(pids) - 1
        while n > 0
            from, data_x = get_msg(:gather, false, tag)
            fromidx = findfirst(sort(pids), from)
            gathered_data[fromidx] = data_x
            n=n-1
        end
        return gathered_data
    else
        send_msg(pid, :gather, x, tag)
        return x
    end
end

function spmd_local(f, ctxt_id, clear_ctxt)
    task_local_storage(:SPMD_CTXT, ctxt_id)
    f()
    clear_ctxt && delete_ctxt_id(ctxt_id)
    return nothing
end

function spmd(f, args...; pids=procs(), context=nothing)
    f_noarg = ()->f(args...)
    clear_ctxt = false
    if context == nothing
        ctxt_id = next_did()
        clear_ctxt = true    # temporary unique context created for this run.
                             # should be cleared at the end of the run.
    else
        ctxt_id = context.id
    end
    @sync for p in pids
        @async remotecall_fetch(spmd_local, p, f_noarg, ctxt_id, clear_ctxt)
    end
    nothing
end

function delete_ctxt_id(ctxt_id)
    global map_ctxts
    haskey(map_ctxts, ctxt_id) && delete!(map_ctxts, ctxt_id)
    nothing
end

function close(ctxt::SPMDContext)
    for p in ctxt.pids
        Base.remote_do(delete_ctxt_id, p, ctxt.id)
    end
    ctxt.release = false
end

end
