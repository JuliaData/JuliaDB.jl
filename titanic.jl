using JuliaDB: Categorical, schema, splitschema, width, tomat

db = load_table(joinpath(homedir(), "Downloads/titanic.csv"),
                indexcols=["PassengerId"])

sch = JuliaDB.schema(db)

sch[:Survived] = Categorical([0, 1])

tomat(sch, db)

# At this point you'd want to save the schema to disk somewhere

xsch, ysch = splitschema(sch, :Survived)

data = ((tomat(xsch, data), tomat(ysch, data))
        for data in Iterators.partition(distribute(db), 10))

# Sample first data point
first(data)

using Flux

model = Chain(
  Dense(width(xsch), 32, relu),
  Dense(32, width(ysch)),
  softmax)

# See an example prediction
model(first(data)[1])

loss(x, y) = Flux.mse(model(x), y)

opt = Flux.ADAM(params(model))
evalcb = Flux.throttle(() -> @show(loss(first(data)...)), 2)

@progress for i = 1:10
  Flux.train!(loss, data, opt, cb = evalcb)
end

model(first(data)[1])



first(data)[2]
