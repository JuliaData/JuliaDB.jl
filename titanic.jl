using JuliaDB
using JuliaDB: Categorical, schema, splitschema, width, featuremat

db = loadtable(joinpath(homedir(), "Downloads/train.csv"),
               escapechar='"',
                indexcols=["PassengerId"])

sch = JuliaDB.schema(db)

sch[:Survived] = Categorical([0, 1])

featuremat(sch, db)

# At this point you'd want to save the schema to disk somewhere

xsch, ysch = splitschema(sch, :Survived)

data = ((featuremat(xsch, data)', featuremat(ysch, data))
        for data in Iterators.partition(distribute(db, 1), 10))

# Sample first data point
@show map(size, first(data))

using Flux

model = Chain(
  Dense(width(xsch), 32, relu),
  Dense(32, width(ysch)),
  softmax)

# See an example prediction
model(first(data)[1])

loss(x, y) = Flux.mse(model(x), y)

opt = Flux.ADAM(Flux.params(model))
evalcb = Flux.throttle(() -> @show(loss(first(data)...)), 2)

for i = 1:10
  Flux.train!(loss, data, opt, cb = evalcb)
end

model(first(data)[1])



first(data)[2]
