using RecipesBase

# threshold to plot partitions rather than data
const PTHRESH = 10_000

#-----------------------------------------------------------------# table and selection
@recipe function f(t::AbstractIndexedTable, selection; partition::Bool = length(t) > PTHRESH)
    if partition
        reducer = make_reducer(t, selection)
        @series begin 
            title --> selection 
            reduce(reducer, t; select = selection)
        end
    else
        @series begin 
            title --> selection
            collect(select(t, selection))
        end
    end
end

function make_reducer(t::AbstractIndexedTable, selection)
    Partition(Mean())
end

#--------------------------------------------------------------------# table by itself
@recipe function f(t::AbstractIndexedTable; forceplot = false)
    (length(t) > PTHRESH || forceplot) && error("Table is too big. Override error with `forceplot = true`.")
    cnames = colnames(t)
    layout --> length(cnames)
    label --> hcat(cnames...)
    for nm in cnames
        @series begin 
            select(t, nm)
        end
    end
end