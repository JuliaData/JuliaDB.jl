```@meta
CurrentModule = JuliaDB
DocTestSetup = quote
    using JuliaDB
    import JuliaDB: ML
end
```
# Feature Extraction

Machine learning models are composed of mathematical operations on matrices of numbers. However, data in the real world is often in tabular form containing more than just numbers. Hence, the first step in applying machine learning is to turn such tabular non-numeric data into a matrix of numbers. Such matrices are called "feature matrices". JuliaDB contains an `ML` module which has helper functions to extract feature matrices.

In this document, we will turn the [titanic dataset from Kaggle](https://www.kaggle.com/c/titanic) into numeric form and apply a machine learning model on it.

```@example titanic
using JuliaDB

download("https://raw.githubusercontent.com/agconti/"*
          "kaggle-titanic/master/data/train.csv", "train.csv")

train_table = loadtable("train.csv", escapechar='"')
popcol(popcol(popcol(train_table, :Name), :Ticket), :Cabin) # hide
```

## ML.schema

Schema is a programmatic description of the data in each column. It is a dictionary which maps each column (by name) to its schema type (mainly `Continuous`, and `Categorical`).

- `ML.Continuous`: data is drawn from the real number line (e.g. Age)
- `ML.Categorical`: data is drawn from a fixed set of values (e.g. Sex)

`ML.schema(train_table)` will go through the data and infer the types and distribution of data. Let's try it without any arguments on the titanic dataset:

```@example titanic
ML.schema(train_table)
```

Here is how the schema was inferred:

- Numeric fields were inferred to be `Continuous`, their mean and standard deviations were computed. This will later be used in normalizing the column in the feature matrix using the formula `((value - mean) / standard_deviation)`. This will bring all columns to the same "scale" making the training more effective.
- Some string columns are inferred to be `Categorical` (e.g. Sex, Embarked) - this means that the column is a [PooledArray](https://github.com/JuliaComputing/PooledArrays.jl), and is drawn from a small "pool" of values. For example Sex is either "male" or "female"; Embarked is one of "Q", "S", "C" or ""
- Some string columns (e.g. Name) get the schema `nothing` -- such columns usually contain unique identifying data, so are not useful in machine learning.
- The age column was inferred as `Maybe{Continuous}` -- this means that there are missing values in the column. The mean and standard deviation computed are for the non-missing values.

You may note that `Survived` column contains only 1s and 0s to denote whether a passenger survived the disaster or not. However, our schema inferred the column to be `Continuous`. To not be overly presumptive `ML.schema` will assume all numeric columns are continuous by default. We can give the hint that the Survived column is categorical by passing the `hints` arguemnt as a dictionary of column name to schema type. Further, we will also treat `Pclass` (passenger class) as categorical and suppress `Parch` and `SibSp` fields.

```@example titanic
sch = ML.schema(train_table, hints=Dict(
        :Pclass => ML.Categorical,
        :Survived => ML.Categorical,
        :Parch => nothing,
        :SibSp => nothing,
        :Fare => nothing,
        )
)
```

## Split schema into input and output

In a machine learning model, a subset of fields act as the input to the model, and one or more fields act as the output (predicted variables). For example, in the titanic dataset, you may want to predict whether a person will survive or not. So "Survived" field will be the output column. Using the `ML.splitschema` function, you can split the schema into input and output schema.

```@example titanic
input_sch, output_sch = ML.splitschema(sch, :Survived)
```

## Extracting feature matrix

Once the schema has been created, you can extract the feature matrix according to the given schema using `ML.featuremat`:

```@example titanic
train_input = ML.featuremat(input_sch, train_table)
```

```@example titanic
train_output = ML.featuremat(output_sch, train_table)
```

## Learning

Let us create a simple neural network to learn whether a passenger will survive or not using the [Flux](https://fluxml.github.io/) framework.

`ML.width(schema)` will give the number of features in the `schema` we will use this in specifying the model size:

```@example titanic
using Flux

model = Chain(
  Dense(ML.width(input_sch), 32, relu),
  Dense(32, ML.width(output_sch)),
  softmax)

loss(x, y) = Flux.mse(model(x), y)
opt = Flux.ADAM(Flux.params(model))
evalcb = Flux.throttle(() -> @show(loss(first(data)...)), 2);
```

Train the data in 10 iterations

```@example titanic
data = [(train_input, train_output)]
for i = 1:10
  Flux.train!(loss, data, opt, cb = evalcb)
end
```

`data` given to the model is a vector of batches of input-output matrices. In this case we are training with just 1 batch.

## Prediction

Now let's load some testing data to use the model we learned to predict survival.

```@example titanic

download("https://raw.githubusercontent.com/agconti/"*
          "kaggle-titanic/master/data/test.csv", "test.csv")

test_table = loadtable("test.csv", escapechar='"')

test_input = ML.featuremat(input_sch, test_table) ;
```

Run the model on one observation:

```@example titanic
model(test_input[:, 1])
```
The output has two numbers which add up to 1: the probability of not surviving vs that of surviving. It seems, according to our model, that this person is unlikely to survive on the titanic.

You can also run the model on all observations by simply passing the whole feature matrix to `model`.

```@example titanic
model(test_input)
```
