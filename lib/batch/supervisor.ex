defmodule Batch.Supervisor do
  @moduledoc """
  A batch supervisor.

  This module defines a supervisor which can be used to dynamically supervise batches.

  `start_link/1` can be used to start the supervisor. See the `Batch` module for more
  examples.

  ## Name registration

  A `Batch.Supervisor` is bound to the same name registration rules as a `GenServer`.
  Read more about them in the `GenServer` docs.
  """

  @typedoc "Option values used by `start_link`"
  @type option :: Supervisor.option |
                  {:restart, :supervisor.restart} |
                  {:shutdown, :supervisor.shutdown}

  @doc false
  def child_spec(arg) do
    %{
      id: Batch.Supervivsor,
      start: {Batch.Supervisor, :start_link, [arg]},
      type: :supervisor
    }
  end

  @doc """
  Starts a new supervisor.

  The supported options are:

    * `:name` - used to register a supervisor name, the supported values are described under
      the `Name Registration` section in the `GenServer` module docs;

    * `:restart` - the restart strategy, may be `:temporary` (the default), `:transient` or
      `:permanent`. `:temporary` means the batch is never restarted, `:transient` means it is
      restarted if the exit is not `:normal`, `:shutdown` or `{:shutdown, reason}`. A
      `:permanent` restart strategy means it is always restarted. It defaults to `:temporary`
      so batches aren't automatically restarted when they complete nor in case of crashes. Note
      the `:async` functions in this module support only `:temporary` restarts;

    * `:shutdown` - `:brutal_kill` if the batch must be killed directly on
      shutdown or an integer indicating the timeout value, defaults to 5000 milliseconds;

    * `:max_restarts` and `:max_seconds` - as specified in `Supervisor`;

  """
  @spec start_link([option]) :: Supervisor.on_start
  def start_link(opts \\ []), do: Task.Supervisor.start_link(opts)

  @doc """
  Starts a batch that can be awaited on.

  The `supervisor` must be a reference as defined in `Batch.Supervisor`. The batch will still
  be linked to the caller, see `Batch.async/1` for more information and `async_nolink/2` for a
  non-linked variant.

  Note this function requires the batch supervisor to have `:temporary` as the `:restart`
  option (the default), as `async/2` keeps a direct reference to the batch which is
  lost if the batch is restarted.
  """
  @spec async(Supervisor.supervisor, [function | mfa]) :: Batch.t
  def async(supervisor, functions) do
    task = Task.Supervisor.async(supervisor, Batch.function(functions))
    struct(Batch, Map.from_struct(task))
  end

  @doc """
  Starts a batch that can be awaited on.

  The `supervisor` must be a reference as defined in `Batch.Supervisor`. The batch won't be
  linked to the caller, see `Batch.async/1` for more information.

  Note this function requires the batch supervisor to have `:temporary` as the `:restart`
  option (the default), as `async_nolink/2` keeps a direct reference to the batch which
  is lost if the batch is restarted.

  ## Compatibility with OTP behaviours

  If you create a batch using `async_nolink` inside an OTP behaviour like `GenServer`, you
  should match on the message coming from the batch inside your `c:GenServer.handle_info/2`
  callback.

  The reply sent by the batch will be in the format `{ref, result}`, where `ref` is the
  monitor reference held by the batch struct and `result` is the return value of the batch
  functions.

  Keep in mind that, regardless of how the batch created with `async_nolink` terminates, the
  caller's process will always receive a `:DOWN` message with the same `ref` value that is
  held by the batch struct. If the batch terminates normally, the reason in the `:DOWN`
  message will be `:normal`.
  """
  @spec async_nolink(Supervisor.supervisor, [function | mfa]) :: Batch.t
  def async_nolink(supervisor, functions) do
    task = Task.Supervisor.async_nolink(supervisor, Batch.function(functions))
    struct(Batch, Map.from_struct(task))
  end

  @doc """
  Terminates the child with the given `pid`.
  """
  @spec terminate_child(Supervisor.supervisor, pid) :: :ok
  def terminate_child(supervisor, pid) when is_pid(pid) do
    Task.Supervisor.terminate_child(supervisor, pid)
  end

  @doc """
  Returns all children PIDs.
  """
  @spec children(Supervisor.supervisor) :: [pid]
  def children(supervisor), do: Task.Supervisor.children(supervisor)

  @doc """
  Starts a batch as a child of the given `supervisor`.

  Note that the spawned process is not linked to the caller, but only to the supervisor. This
  command is useful in case the batch needs to perform side-effects (like I/O) and does not
  need to report back to the caller.
  """
  @spec start_child(Supervisor.supervisor, [function | mfa]) :: {:ok, pid}
  def start_child(supervisor, functions) do
    Task.Supervisor.start_child(supervisor, Batch.function(functions))
  end
end
