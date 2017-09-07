defmodule Batch do
  @moduledoc """
  Defines a batch.

  Batches are processes meant to execute several functions asynchronously, collecting
  their return values or returning the error of the first failure (fail-fast).

      batch = Batch.async([
        fn -> Blog.get_post(post_id) end,
        fn -> Blog.get_comments(post_id) end
      ])

      {:ok, [post, comments]} = Batch.await(batch)

  As shown in the example above, batches spawned with `async` can be awaited on by their caller
  process (and only their caller). They are implemented by spawning a process that sends
  a message to the caller once the given computation is performed. Internally, the batch
  process spawns a new process per function (linked and monitored by the batch process) and
  awaits for replies.

  Successful computations are represented with the `:ok` atom or the `{:ok, result}` tuple.

      batch = Batch.async([
        fn -> {:ok, 1 + 1} end,
        fn -> :ok end,
        {Enum, :fetch, [[2, 4, 6], 1]}
      ])

      {:ok, [{:ok, 2}, :ok, {:ok, 4}]} = Batch.await(batch)

  Any other value will be considered a failure. Failures and exits are reported immediately
  (fail-fast), shutting down all the processes.

      batch = Batch.async([
        fn -> {:ok, 1 + 1} end,
        fn -> {:ok, 2 + 2} end,
        fn -> :error end
      ])

      {:error, :error} = Batch.await(batch)

  Batches can also be started as part of a supervision tree and dynamically spawned on
  remote nodes. We will explore all three scenarios next.

  ## Supervised batches

  It is also possible to spawn a batch under a supervisor. It is often done by defining the
  batch in its own module:

      defmodule MyBatch do
        use Batch

        def start_link(arg) do
          Batch.start_link([
            {__MODULE__, :do_something, [arg]},
            {__MODULE__, :do_something_else, [arg]},
          ])
        end

        def do_something(arg) do
          # ...
        end

        def do_something_else(arg) do
          # ...
        end
      end

  And then passing it to the supervisor:

      Supervisor.start_link([MyBatch])

  Since these batches are supervised and not directly linked to the caller, they cannot be
  awaited on. Note `start_link/1`, unlike `async/1`, returns `{:ok, pid}` (which is the result
  expected by supervisors).

  Note `use Batch` defines a `child_spec/1` function, allowing the defined module to be put
  under a supervision tree. The generated `child_spec/1` can be customized with the following
  options:

    * `:id` - the child specification id, defaults to the current module
    * `:start` - how to start the child process (defaults to calling `__MODULE__.start_link/1`)
    * `:restart` - when the child should be restarted, defaults to `:temporary`
    * `:shutdown` - how to shut down the child

  As `Task`, a `Batch` has a default `:restart` of `:temporary`. This means the batch will not
  be restarted even if it crashes. If you desire the batch to be restarted for non-successful
  exists, do:

      use Batch, restart: :transient

  If you want the batch to always be restarted:

      use Batch, restart: :permanent

  See the `Supervisor` docs for more information.

  ## Dynamically supervised batches

  The `Batch.Supervisor` module allows developers to dynamically create multiple supervised
  batches.

  A short example is:

      {:ok, pid} = Batch.Supervisor.start_link()
      batch = Batch.Supervisor.async(pid, [
        fn ->
          # Do something
        end,
        fn ->
          # Do something else
        end
      ])
      Batch.await(batch)

  However, in the majority of cases, you want to add the batch supervisor to your
  supervision tree:

      Supervisor.start_link([
        {Batch.Supervisor, name: MyApp.BatchSupervisor}
      ])

  Now you can dynamically start supervised batches:

      Batch.Supervisor.start_child(MyApp.BatchSupervisor, [
        fn ->
          # Do something
        end,
        fn ->
          # Do something else
        end
      ])

  Or even use the async/await pattern:

      Batch.Supervisor.async(MyApp.BatchSupervisor, [
        fn ->
          # Do something
        end,
        fn ->
          # Do something else
        end
      ]) |> Batch.await()

  Finally, check `Batch.Supervisor` for other supported operations.

  ## Distributed batches

  Since `Batch` provides a supervisor, it is easy to use one to dynamically spawn
  batches across nodes:

      # On the remote node
      Batch.Supervisor.start_link(name: MyApp.DistSupervisor)

      # On the client
      Batch.Supervisor.async({MyApp.DistSupervisor, :remote@local},
                              MyMod, :my_fun, [arg1, arg2, arg3])

  Note that, when working with distributed batches, one should express the list of functions
  using explicit module, function and arguments, instead of anonymous functions. That's because
  anonymous functions expect the same module version to exist on all involved nodes. Check the
  `Agent` module documentation for more information on distributed processes as the limitations
  described there apply to the whole ecosystem.
  """

  @doc """
  The Batch struct.

  It contains these fields:

    * `:pid` - the PID of the batch process; `nil` if the batch does not use a batch process

    * `:ref` - the batch monitor reference

    * `:owner` - the PID of the process that started the batch

  """
  defstruct pid: nil, ref: nil, owner: nil

  @type t :: %__MODULE__{}

  @doc false
  def child_spec(arg) do
    %{
      id: Batch,
      start: {Batch, :start_link, [arg]},
      restart: :temporary
    }
  end

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      spec = [
        id: opts[:id] || __MODULE__,
        start: Macro.escape(opts[:start]) || quote(do: {__MODULE__, :start_link, [arg]}),
        restart: opts[:restart] || :temporary,
        shutdown: opts[:shutdown] || 5000,
        type: :worker
      ]

      @doc false
      def child_spec(arg) do
        %{unquote_splicing(spec)}
      end

      defoverridable child_spec: 1
    end
  end

  @doc """
  Starts a process linked to the current process.

  This is often used to start the process as part of a supervision tree.
  """
  @spec start_link([function | mfa]) :: {:ok, pid}
  def start_link(functions), do: Task.start_link(function(functions))

  @doc """
  Starts a batch.

  This is only used when the batch is used for side-effects (i.e. no interest in the
  returned result) and it should not be linked to the current process.
  """
  @spec start([function | mfa]) :: {:ok, pid}
  def start(functions), do: Task.start(function(functions))

  @doc """
  Starts a batch that must be awaited on.

  A `Batch` struct is returned containing the relevant information. Developers must eventually
  call `Batch.await/2` or `Batch.yield/2` followed by `Batch.shutdown/2` on the returned batch.

  ## Linking

  As with `Task.async/1` or `Task.async/3`, this function spawns a process that is linked to
  and monitored by the caller process. The linking part is important because it aborts the
  batch if the parent process dies. It also guarantees the code before async/await has the
  same properties after you add the async call.

  If you don't want to link the caller to the batch, then you must use a supervised batch with
  `Batch.Supervisor` and call `Batch.Supervisor.async_nolink/2`.

  In any case, avoid any of the following:

    * Setting `:trap_exit` to `true` - trapping exits should be used only in special
      circumstances as it would make your process immune to not only exits from the batch
      but from any other processes.

      Moreover, even when trapping exits, calling `await` will still exit if the batch has
      terminated without sending its results back.

    * Unlinking the batch process started with `async`/`await`. If you unlink the processes
      and the batch does not belong to any supervisor, you may leave dangling batches in
      case the parent dies.

  ## Message format

  The reply sent by the batch will be in the format `{ref, result}`, where `ref` is the
  monitor reference held by the batch struct and `result` is the return value of the
  batch operation.
  """
  @spec async([function | mfa]) :: t
  def async(functions) do
    task = Task.async(function(functions))
    struct(Batch, Map.from_struct(task))
  end

  @doc """
  Awaits a batch reply and returns it.

  Returns `{:ok, results}` in case of success, or `{:error, error}` otherwise. Results are
  returned in the same order as their corresponding functions.

  A timeout, in milliseconds, can be given with default value of `5000`. In case the batch
  process dies, this function will exit with the same reason as the batch.

  If the timeout is exceeded, `await` will exit; however, the batch will continue to run.
  When the calling process exits, its exit signal will terminate the batch if it is not
  trapping exits.

  This function assumes the batch's monitor is still active or the monitor's `:DOWN` message
  is in the message queue. If it has been demonitored, or the message already received, this
  function will wait for the duration of the timeout awaiting the message.

  This function can only be called once for any given batch. If you want to be able to check
  multiple times if a long-running batch has finished its computation, use `yield/2` instead.

  ## Compatibility with OTP behaviours

  It is not recommended to `await` a long-running batch inside an OTP behaviour such as
  `GenServer`. Instead, you should match on the message coming from a batch inside your
  `GenServer.handle_info/2` callback.

  ## Examples

      iex> batch = Batch.async([fn -> {:ok, 1 + 1} end, fn -> :ok end, fn -> {:ok, 2 + 2} end])
      iex> Batch.await(batch)
      {:ok, [{:ok, 2}, :ok, {:ok, 4}]}

  """
  @spec await(t, timeout) :: {:ok, term} | {:error, term} | no_return
  def await(batch, timeout \\ 5_000)

  def await(%Batch{owner: owner} = batch, _) when owner != self() do
    raise(ArgumentError, invalid_owner_error(batch))
  end

  def await(%Batch{} = batch, timeout) do
    task = struct(Task, Map.from_struct(batch))
    Task.await(task, timeout)
  catch
    :exit, {reason, _} -> exit({reason, {__MODULE__, :await, [batch, timeout]}})
  end

  @doc ~S"""
  Temporarily blocks the current process waiting for a batch reply.

  Returns `{:ok, results}` or `{:error, error}` if the reply is received, `nil` if no reply has
  arrived, or `{:exit, reason}` if the batch has already exited. Keep in mind that normally
  a batch failure also causes the process owning the batch to exit. Therefore this
  function can return `{:exit, reason}` only if

    * the batch process exited with the reason `:normal`
    * it isn't linked to the caller
    * the caller is trapping exits

  A timeout, in milliseconds, can be given with default value of `5000`. If the time runs out
  before a message from the batch is received, this function will return `nil` and the monitor
  will remain active. Therefore `yield/2` can be called multiple times on the same batch.

  This function assumes the batch's monitor is still active or the monitor's `:DOWN`
  message is in the message queue. If it has been demonitored or the message already received,
  this function will wait for the duration of the timeout awaiting the message.

  If you intend to shut the batch down if it has not responded within `timeout`
  milliseconds, you should chain this together with `shutdown/1`, like so:

      case Batch.yield(batch, timeout) || Batch.shutdown(batch) do
        {:ok, results} ->
          results
        {:error, error} ->
          Logger.error "Batch failed with #{inspect error}"
          error
        nil ->
          Logger.warn "Failed to get the results in #{timeout}ms"
          nil
      end

  That ensures that if the batch completes after the `timeout` but before `shutdown/1`
  has been called, you will still get the result, since `shutdown/1` is designed to
  handle this case and return the result.

  ## Examples

      iex> batch = Batch.async([fn -> {:ok, 1 + 1} end, fn -> :ok end, fn -> {:ok, 2 + 2} end])
      iex> Batch.yield(batch)
      {:ok, [{:ok, 2}, :ok, {:ok, 4}]}

      iex> batch = Batch.async([fn -> {:ok, 1 + 1} end, fn -> :error end])
      iex> Batch.yield(batch)
      {:error, :error}

  """
  @spec yield(t, timeout) :: {:ok, term} | {:error, term} | {:exit, term} | nil
  def yield(batch, timeout \\ 5_000)

  def yield(%Batch{owner: owner} = batch, _) when owner != self() do
    raise(ArgumentError, invalid_owner_error(batch))
  end

  def yield(%Batch{} = batch, timeout) do
    task = struct(Task, Map.from_struct(batch))
    case Task.yield(task, timeout) do
      {:ok, reply} -> reply
      {:exit, reason} -> {:exit, reason}
      nil -> nil
    end
  catch
    :exit, {reason, _} -> exit({reason, {__MODULE__, :yield, [batch, timeout]}})
  end

  @doc """
  Unlinks and shuts down the batch, and then checks for a reply.

  Returns `{:ok, results}` or `{:error, error}` if the reply is received while shutting down
  the batch, `{:exit, reason}` if the batch died, otherwise `nil`.

  The shutdown method is either a timeout or `:brutal_kill`. In case of a `timeout`, a
  `:shutdown` exit signal is sent to the batch process and if it does not exit within the
  timeout, it is killed. With `:brutal_kill` the batch is killed straight away. In case the
  batch terminates abnormally (possibly killed by another process), this function will exit
  with the same reason.

  It is not required to call this function when terminating the caller, unless exiting with
  reason `:normal` or if the batch is trapping exits. If the caller is exiting with a
  reason other than `:normal` and the batch is not trapping exits, the caller's exit signal
  will stop the batch. The caller can exit with reason `:shutdown` to shutdown all of its
  linked processes, including batches, that are not trapping exits without generating any
  log messages.

  If a batch's monitor has already been demonitored or received and there is not a response
  waiting in the message queue this function will return `{:exit, :noproc}` as the result or
  exit reason can not be determined.
  """
  @spec shutdown(t, timeout | :brutal_kill) :: {:ok, term} | {:error, term} | {:exit, term} | nil
  def shutdown(batch, shutdown \\ 5_000)

  def shutdown(%Batch{pid: nil} = batch, _) do
    raise(ArgumentError, "batch #{inspect batch} does not have an associated batch process")
  end

  def shutdown(%Batch{owner: owner} = batch, _) when owner != self() do
    raise(ArgumentError, invalid_owner_error(batch))
  end

  def shutdown(%Batch{} = batch, timeout) do
    task = struct(Task, Map.from_struct(batch))
    case Task.shutdown(task, timeout) do
      {:ok, reply} -> reply
      {:exit, reason} -> {:exit, reason}
      nil -> nil
    end
  catch
    :exit, {reason, _} -> exit({reason, {__MODULE__, :shutdown, [batch, timeout]}})
  end

  @doc false
  def function(functions) do
    fn ->
      tasks = Enum.map(functions, fn {m, f, a} -> Task.async(m, f, a)
                                     fun -> Task.async(fun) end)

      with {:ok, results} <- Batch.collect_results(tasks) do
        {:ok, Enum.map(tasks, &find_result(results, &1))}
      end
    end
  end

  @doc false
  def collect_results(tasks, results \\ [])
  def collect_results([], results), do: {:ok, results}
  def collect_results(tasks, results) do
    receive do
      {ref, {:ok, reply}} ->
        Process.demonitor(ref, [:flush])
        task = find_task(tasks, ref)
        collect_results(tasks -- [task], [{task, {:ok, reply}} | results])

      {ref, :ok} ->
        Process.demonitor(ref, [:flush])
        task = find_task(tasks, ref)
        collect_results(tasks -- [task], [{task, :ok} | results])

      {_ref, reply} ->
        Enum.each(tasks, &Task.shutdown(&1))
        {:error, reply}

      {:DOWN, _ref, _, proc, reason} ->
        Enum.each(tasks, &Task.shutdown(&1))
        exit(reason(reason, proc))
    end
  end

  defp find_task(tasks, ref), do: Enum.find(tasks, fn task -> task.ref == ref end)

  defp find_result(results, task) do
    {_task, result} = Enum.find(results, fn {t, _result} -> task == t end)
    result
  end

  defp reason(:noconnection, proc), do: {:nodedown, monitor_node(proc)}
  defp reason(reason, _), do: reason

  defp monitor_node(pid) when is_pid(pid), do: node(pid)
  defp monitor_node({_, node}), do: node

  defp invalid_owner_error(batch) do
    "batch #{inspect batch} must be queried from the owner but was queried from #{inspect self()}"
  end
end
