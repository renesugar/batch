# Batch

[![Build Status](https://travis-ci.org/fertapric/batch.svg?branch=master)](https://travis-ci.org/fertapric/batch)

Batches are processes meant to execute several functions asynchronously, collecting
their return values or returning the error of the first failure (fail-fast).

## Installation

Add `batch` to your project's dependencies in `mix.exs`:

```elixir
def deps do
  [{:batch, "~> 0.1"}]
end
```

And fetch your project's dependencies:

```shell
$ mix deps.get
```

## Usage

Let's start with an example:

```elixir
batch = Batch.async([
  fn -> Blog.get_post(post_id) end,
  fn -> Blog.get_comments(post_id) end
])

{:ok, [post, comments]} = Batch.await(batch)
```

As shown in the example above, batches spawned with `async` can be awaited on by their caller process (and only their caller). They are implemented by spawning a process that sends a message to the caller once the given computation is performed. Internally, the batch process spawns a new process per function (linked and monitored by the batch process) and awaits for replies.

Successful computations are represented with the `:ok` atom or the `{:ok, result}` tuple.

```elixir
batch = Batch.async([
  fn -> {:ok, 1 + 1} end,
  fn -> :ok end,
  {Enum, :fetch, [[2, 4, 6], 1]}
])

{:ok, [{:ok, 2}, :ok, {:ok, 4}]} = Batch.await(batch)
```

Any other value will be considered a failure. Failures and exits are reported immediately (fail-fast), shutting down all the processes.

```elixir
batch = Batch.async([
  fn -> {:ok, 1 + 1} end,
  fn -> {:ok, 2 + 2} end,
  fn -> :error end
])

{:error, :error} = Batch.await(batch)
```

Batches can also be started as part of a supervision tree and dynamically spawned on remote nodes. We will explore all three scenarios next. [Check the documentation](https://hexdocs.pm/batch) for more information.

## Documentation

Documentation is available at https://hexdocs.pm/batch

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/fertapric/batch. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

### Running tests

Clone the repo and fetch its dependencies:

```shell
$ git clone https://github.com/fertapric/batch.git
$ cd batch
$ mix deps.get
$ mix test
```

### Building docs

```shell
$ mix docs
```

## Copyright and License

Copyright 2017 Fernando Tapia Rico

Batch source code is licensed under the [MIT License](LICENSE).
