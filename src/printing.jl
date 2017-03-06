const TextMIME = Union{MIME"text/plain", MIME"text/html"}
function take_n(t::DTable, n, dir=1)
    i = dir == -1 ? length(t.chunks) : 1
    chunkcol = chunks(t).data.columns.chunk
    top = gather(chunkcol[i])
    while length(top) < n && 1 <= i <= length(chunkcol)
        i += dir
        top = merge(top, gather(chunkcol[i]))
    end
    return top
end

function Base.show(io::IO, t::DTable)
    # we fetch at most 21 elements and let NDSparse
    # display it.
    if !isempty(t.chunks)
        show(io, merge(take_n(t, 11), take_n(t, 10, -1)))
    else
        println(io, "an empty table")
    end
end
