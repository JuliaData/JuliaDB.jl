@widget wdg function interactivepartition(t; throttle = 0.1)
    :x =  @nodeps dropdown(colnames(t), label = "x")
    :y =  @nodeps dropdown(vcat(Symbol(""), colnames(t)), label = "y")
    :nparts =  @nodeps slider(1:200, value = 100, label = "number of partitions")
    :nparts_throttle = Observables.throttle(throttle, :nparts)
    :dropmissing = @nodeps toggle(false, label = "dropmissing")
    :by = @nodeps dropdown(colnames(t), multiple = true)
    wdg[:split] = @nodeps togglecontent(div("by", wdg[:by]), value = false, label = "spit data")
    :plot = @nodeps button("plot")
     @output! wdg begin
        $(:plot)
        y = :y[] == Symbol("") ? [] : [:y[]]
        by = (!(:split[]) || isempty(:by[])) ? nothing : Tuple(:by[])
        partitionplot(t, :x[], y..., by = by, nparts = $(:nparts_throttle), dropmissing = :dropmissing[])
     end
    @layout! wdg begin
        controls = div(:x, :y,  :split, :dropmissing, :plot)
        div(controls, div(_.output, :nparts), style = Dict("display" => "flex", "flex-direction"=>"row"))
    end
end
