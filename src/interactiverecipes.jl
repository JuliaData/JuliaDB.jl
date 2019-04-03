import Observables, Widgets
using Widgets: Widget, @layout!, dropdown, slider, toggle, togglecontent, button

function interactivepartition(t; throttle = 0.1)
    ns = collect(colnames(t))
    x_menu =  dropdown(ns, label = "x")
    y_menu =  dropdown(vcat(Symbol(""), ns), label = "y")
    nparts =  slider(1:200, value = 100, label = "number of partitions")
    nparts_throttle = Observables.throttle(throttle, nparts)
    dropmissing = toggle(false, label = "dropmissing")
    by_menu = dropdown(ns, multiple = true)
    split = togglecontent(Widgets.div("by", by_menu), value = false, label = "spit data")
    plot_button = button("plot")
    output = map(plot_button, nparts_throttle) do _, np
        y = y_menu[] == Symbol("") ? [] : [y_menu[]]
        by = (!split[] || isempty(by_menu[])) ? nothing : Tuple(by_menu[])
        partitionplot(t, x_menu[], y..., by = by, nparts = np, dropmissing = dropmissing[])
    end

    wdg = Widget{:interactivepartition}(
        [:x => x_menu,
         :y => y_menu,
         :nparts => nparts,
         :nparts_throttle => nparts_throttle,
         :split => split,
         :dropmissing => dropmissing,
         :plot_button => plot_button,
         :by => by_menu],
        output = output
    )

    @layout! wdg begin
        controls = Widgets.div(:x, :y,  :split, :dropmissing, :plot_button)
        Widgets.div(controls, Widgets.div(_.output, :nparts), style = Dict("display" => "flex", "flex-direction"=>"row"))
    end
end
