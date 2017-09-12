defmodule BatchTest do
  use ExUnit.Case

  @moduletag :capture_log

  doctest Batch

  def delayed_echo(delay, value) do
    :timer.sleep(delay)
    value
  end

  defp pid_registry(agent, registry_name) do
    pid = self()
    Agent.update(agent, &Map.merge(&1, %{registry_name => pid}))
    :ok
  end

  defp create_dummy_batch(reason) do
    {pid, ref} = spawn_monitor(Kernel, :exit, [reason])

    receive do
      {:DOWN, ^ref, _, _, _} -> %Batch{ref: ref, pid: pid, owner: self()}
    end
  end

  defp create_batch_in_other_process do
    caller = self()

    spawn fn ->
      batch = Batch.async([fn -> :ok end])
      send(caller, batch)
    end

    receive do: (batch -> batch)
  end

  test "can be supervised directly" do
    import Supervisor.Spec

    children = [
      worker(Batch, [[[fn -> :ok end]]])
    ]

    assert {:ok, _} = Supervisor.start_link(children, strategy: :one_for_one)
  end

  test "generates child_spec/1" do
    defmodule MyBatch do
      use Batch
    end

    assert MyBatch.child_spec([{:timer, :sleep, [0]}]) == %{
      id: MyBatch,
      restart: :temporary,
      shutdown: 5000,
      start: {MyBatch, :start_link, [[{:timer, :sleep, [0]}]]},
      type: :worker
    }

    defmodule CustomBatch do
      use Batch,
        id: :id,
        restart: :permanent,
        shutdown: :infinity,
        start: {:foo, :bar, []}
    end

    assert CustomBatch.child_spec([{:timer, :sleep, [0]}]) == %{
      id: :id,
      restart: :permanent,
      shutdown: :infinity,
      start: {:foo, :bar, []},
      type: :worker
    }
  end

  test "returns results in order" do
    batch = Batch.async([
      fn -> delayed_echo(4, :ok) end,
      {__MODULE__, :delayed_echo, [3, {:ok, 1}]},
      fn -> delayed_echo(1, {:ok, 2}) end,
      fn -> delayed_echo(2, {:ok, 3}) end,
    ])

    assert {:ok, [:ok, {:ok, 1}, {:ok, 2}, {:ok, 3}]} == Batch.await(batch)
  end

  test "kills other processes on failure" do
    {:ok, agent} = Agent.start_link(fn -> %{} end)

    batch = Batch.async([
      fn -> delayed_echo(1, pid_registry(agent, :first)) end,
      fn -> delayed_echo(1, pid_registry(agent, :second)) end,
      fn ->
        :timer.sleep(3)
        pid_registry(agent, :third)
      end,
      fn -> delayed_echo(2, :error) end
    ])

    :timer.sleep(2)
    pids = Agent.get(agent, &(&1))

    assert {:error, :error} == Batch.await(batch)
    refute Process.alive?(pids.first)
    refute Process.alive?(pids.second)
    refute Map.has_key?(pids, :third)

    :ok = Agent.stop(agent)
  end

  test "async/1" do
    {:ok, agent} = Agent.start_link(fn -> [] end)

    batch = Batch.async([
      fn -> Agent.get_and_update(agent, fn state -> {{:ok, 1}, [1 | state]} end) end,
      {Agent, :get_and_update, [agent, fn state -> {{:ok, 2}, [2 | state]} end]}
    ])

    # Assert the struct
    assert batch.__struct__ == Batch
    assert is_pid(batch.pid)
    assert is_reference(batch.ref)

    # Assert the link
    {:links, links} = Process.info(self(), :links)
    assert batch.pid in links

    # Let the batch finish
    :timer.sleep(1)

    # Assert response and monitoring messages
    ref = batch.ref
    assert_receive {^ref, {:ok, [{:ok, 1}, {:ok, 2}]}}
    assert_receive {:DOWN, ^ref, _, _, :normal}

    :ok = Agent.stop(agent)
  end

  test "start/1" do
    {:ok, agent} = Agent.start_link(fn -> [] end)

    {:ok, pid} = Batch.start([
      fn -> Agent.update(agent, &[1 | &1]) end,
      {Agent, :update, [agent, &[2 | &1]]}
    ])

    {:links, links} = Process.info(self(), :links)
    refute pid in links

    :timer.sleep(1)
    assert [1, 2] == Agent.get(agent, fn state -> Enum.sort(state) end)

    :ok = Agent.stop(agent)
  end

  test "start_link/1" do
    {:ok, agent} = Agent.start_link(fn -> [] end)

    {:ok, pid} = Batch.start_link([
      fn -> Agent.update(agent, &[1 | &1]) end,
      {Agent, :update, [agent, &[2 | &1]]}
    ])

    {:links, links} = Process.info(self(), :links)
    assert pid in links

    :timer.sleep(1)
    assert [1, 2] == Agent.get(agent, fn state -> Enum.sort(state) end)

    :ok = Agent.stop(agent)
  end

  test "await/2 exits on timeout" do
    batch = %Batch{ref: make_ref(), owner: self()}
    assert catch_exit(Batch.await(batch, 0)) == {:timeout, {Batch, :await, [batch, 0]}}
  end

  test "await/2 exits on normal exit" do
    batch = Batch.async([fn -> :ok end, fn -> exit(:normal) end])
    assert catch_exit(Batch.await(batch)) == {:normal, {Batch, :await, [batch, 5000]}}
  end

  test "await/2 exits on batch throw" do
    Process.flag(:trap_exit, true)
    batch = Batch.async([fn -> :ok end, fn -> throw(:unknown) end])
    assert {{{:nocatch, :unknown}, _}, {Batch, :await, [^batch, 5000]}} =
           catch_exit(Batch.await(batch))
  end

  test "await/2 exits on batch error" do
    Process.flag(:trap_exit, true)
    batch = Batch.async([fn -> :ok end, fn -> raise("oops") end])
    assert {{%RuntimeError{}, _}, {Batch, :await, [^batch, 5000]}} = catch_exit(Batch.await(batch))
  end

  test "await/2 exits on batch undef module error" do
    Process.flag(:trap_exit, true)
    batch = Batch.async([fn -> :ok end, &:module_does_not_exist.undef/0])
    assert {{:undef, [{:module_does_not_exist, :undef, _, _} | _]},
            {Batch, :await, [^batch, 5000]}} =
           catch_exit(Batch.await(batch))
  end

  test "await/2 exits on batch undef function error" do
    Process.flag(:trap_exit, true)
    batch = Batch.async([fn -> :ok end, &BatchTest.undef/0])
    assert {{:undef, [{BatchTest, :undef, _, _} | _]},
            {Batch, :await, [^batch, 5000]}} =
           catch_exit(Batch.await(batch))
  end

  test "await/2 exits on batch exit" do
    Process.flag(:trap_exit, true)
    batch = Batch.async([fn -> :ok end, fn -> exit(:unknown) end])
    assert {:unknown, {Batch, :await, [^batch, 5000]}} =
           catch_exit(Batch.await(batch))
  end

  test "await/2 exits on :noconnection" do
    ref = make_ref()
    batch = %Batch{ref: ref, pid: self(), owner: self()}
    send self(), {:DOWN, ref, :process, self(), :noconnection}
    assert {{:nodedown, :nonode@nohost}, _} = catch_exit(Batch.await(batch))
  end

  test "await/2 exits on :noconnection from named monitor" do
    ref = make_ref()
    batch = %Batch{ref: ref, pid: nil, owner: self()}
    send self(), {:DOWN, ref, :process, {:name, :node}, :noconnection}
    assert {{:nodedown, :node}, _} = catch_exit(Batch.await(batch))
  end

  test "await/2 raises when invoked from a non-owner process" do
    batch = create_batch_in_other_process()
    message =
      "batch #{inspect batch} must be queried from the owner but was queried from #{inspect self()}"
    assert_raise ArgumentError, message, fn -> Batch.await(batch, 1) end
  end

  test "yield/2 returns {:ok, result} when reply and :DOWN in message queue" do
    batch = %Batch{ref: make_ref(), owner: self()}
    send(self(), {batch.ref, {:ok, [:ok]}})
    send(self(), {:DOWN, batch.ref, :process, self(), :abnormal})
    assert Batch.yield(batch, 0) == {:ok, [:ok]}
    refute_received {:DOWN, _, _, _, _}
  end

  test "yield/2 returns nil on timeout" do
    batch = %Batch{ref: make_ref(), owner: self()}
    assert Batch.yield(batch, 0) == nil
  end

  test "yield/2 return exit on normal exit" do
    batch = Batch.async([fn -> :ok end, fn -> exit(:normal) end])
    assert Batch.yield(batch) == {:exit, :normal}
  end

  describe "yield/2 exits on :noconnection" do
    ref = make_ref()
    batch = %Batch{ref: ref, pid: self(), owner: self()}
    send self(), {:DOWN, ref, self(), self(), :noconnection}
    assert {{:nodedown, :nonode@nohost}, _} = catch_exit(Batch.yield(batch))
  end

  test "yield/2 raises when invoked from a non-owner process" do
    batch = create_batch_in_other_process()
    message =
      "batch #{inspect batch} must be queried from the owner but was queried from #{inspect self()}"
    assert_raise ArgumentError, message, fn -> Batch.yield(batch, 1) end
  end

  describe "shutdown/2 returns {:ok, result} when reply and abnormal :DOWN in message queue" do
    test "with timeout" do
      batch = create_dummy_batch(:abnormal)
      send(self(), {batch.ref, {:ok, [:ok]}})
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :abnormal})
      assert Batch.shutdown(batch) == {:ok, [:ok]}
      refute_received {:DOWN, _, _, _, _}
    end

    test "with :brutal_kill" do
      batch = create_dummy_batch(:abnormal)
      send(self(), {batch.ref, {:ok, [:ok]}})
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :abnormal})
      assert Batch.shutdown(batch, :brutal_kill) == {:ok, [:ok]}
      refute_received {:DOWN, _, _, _, _}
    end
  end

  describe "shutdown/2 returns {:ok, result} when reply and normal :DOWN in message queue" do
    test "with timeout" do
      batch = create_dummy_batch(:normal)
      send(self(), {batch.ref, {:ok, [:ok]}})
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :normal})
      assert Batch.shutdown(batch) == {:ok, [:ok]}
      refute_received {:DOWN, _, _, _, _}
    end

    test "with :brutal_kill" do
      batch = create_dummy_batch(:normal)
      send(self(), {batch.ref, {:ok, [:ok]}})
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :normal})
      assert Batch.shutdown(batch, :brutal_kill) == {:ok, [:ok]}
      refute_received {:DOWN, _, _, _, _}
    end
  end

  describe "shutdown/2 returns {:ok, result} when reply and shutdown :DOWN in message queue" do
    test "with timeout" do
      batch = create_dummy_batch(:shutdown)
      send(self(), {batch.ref, {:ok, [:ok]}})
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :shutdown})
      assert Batch.shutdown(batch) == {:ok, [:ok]}
      refute_received {:DOWN, _, _, _, _}
    end

    test "with :brutal_kill" do
      batch = create_dummy_batch(:shutdown)
      send(self(), {batch.ref, {:ok, [:ok]}})
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :shutdown})
      assert Batch.shutdown(batch, :brutal_kill) == {:ok, [:ok]}
      refute_received {:DOWN, _, _, _, _}
    end
  end

  describe "shutdown/2 returns exit on abnormal :DOWN in message queue" do
    test "with timeout" do
      batch = create_dummy_batch(:abnormal)
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :abnormal})
      assert Batch.shutdown(batch) == {:exit, :abnormal}
    end

    test "with :brutal_kill" do
      batch = create_dummy_batch(:abnormal)
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :abnormal})
      assert Batch.shutdown(batch, :brutal_kill) == {:exit, :abnormal}
    end
  end

  describe "shutdown/2 returns exit on normal :DOWN in message queue" do
    test "with timeout" do
      batch = create_dummy_batch(:normal)
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :normal})
      assert Batch.shutdown(batch) == {:exit, :normal}
    end

    test "with :brutal_kill" do
      batch = create_dummy_batch(:normal)
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :normal})
      assert Batch.shutdown(batch, :brutal_kill) == {:exit, :normal}
    end
  end

  describe "shutdown/2 on shutdown :DOWN in message queue" do
    test "returns nil with timeout" do
      batch = create_dummy_batch(:shutdown)
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :shutdown})
      assert Batch.shutdown(batch) == nil
    end

    test "returns exit with :brutal_kill" do
      batch = create_dummy_batch(:shutdown)
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :shutdown})
      assert Batch.shutdown(batch, :brutal_kill) == {:exit, :shutdown}
    end
  end

  describe "shutdown/2 returns exit on killed :DOWN in message queue" do
    test "returns exit with timeout" do
      batch = create_dummy_batch(:killed)
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :killed})
      assert Batch.shutdown(batch) == {:exit, :killed}
    end

    test "returns nil with :brutal_kill" do
      batch = create_dummy_batch(:killed)
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :killed})
      assert Batch.shutdown(batch, :brutal_kill) == nil
    end
  end

  describe "shutdown/2 exits on noconnection :DOWN in message queue" do
    test "with timeout" do
      batch = create_dummy_batch(:noconnection)
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :noconnection})
      assert catch_exit(Batch.shutdown(batch)) ==
             {{:nodedown, node()}, {Batch, :shutdown, [batch, 5000]}}
    end

    test "with :brutal_kill" do
      batch = create_dummy_batch(:noconnection)
      send(self(), {:DOWN, batch.ref, :process, batch.pid, :noconnection})
      assert catch_exit(Batch.shutdown(batch, :brutal_kill)) ==
             {{:nodedown, node()}, {Batch, :shutdown, [batch, :brutal_kill]}}
    end
  end

  describe "returns {:exit, :noproc} if batch handled" do
    test "with timeout" do
      batch = create_dummy_batch(:noproc)
      assert Batch.shutdown(batch) == {:exit, :noproc}
    end

    test "with :brutal_kill" do
      batch = create_dummy_batch(:noproc)
      assert Batch.shutdown(batch, :brutal_kill) == {:exit, :noproc}
    end
  end

  test "shutdown/2 raises if batch PID is nil" do
    batch = %Batch{ref: make_ref(), pid: nil}
    message = "batch #{inspect batch} does not have an associated batch process"
    assert_raise ArgumentError, message, fn -> Batch.shutdown(batch) end
  end

  test "shutdown/2 raises when invoked from a non-owner process" do
    batch = create_batch_in_other_process()
    message =
      "batch #{inspect batch} must be queried from the owner but was queried from #{inspect self()}"
    assert_raise ArgumentError, message, fn -> Batch.shutdown(batch) end
  end

  describe "shutdown/2 returns nil on shutting down batch" do
    test "with timeout" do
      batch = Batch.async([fn -> :ok end, {:timer, :sleep, [:infinity]}])
      assert Batch.shutdown(batch) == nil
    end

    test "with :brutal_kill" do
      batch = Batch.async([fn -> :ok end, {:timer, :sleep, [:infinity]}])
      assert Batch.shutdown(batch, :brutal_kill) == nil
    end
  end

  test "shutdown/2 returns nil after shutdown timeout" do
    {:ok, agent} = Agent.start_link(fn -> [] end)

    batch = Batch.async([
      fn ->
        Process.flag(:trap_exit, true)
        Agent.update(agent, &[1 | &1])
        Process.sleep(:infinity)
      end,
      fn ->
        Process.flag(:trap_exit, true)
        Agent.update(agent, &[2 | &1])
        Process.sleep(:infinity)
      end,
    ])

    :timer.sleep(1)
    assert [1, 2] == Agent.get(agent, fn state -> Enum.sort(state) end)

    assert Batch.shutdown(batch, 1) == nil
    refute_received {:DOWN, _, _, _, _}

    :ok = Agent.stop(agent)
  end

  test "shutdown/2 with :brutal_kill returns nil on killing batch" do
    {:ok, agent} = Agent.start_link(fn -> [] end)

    batch = Batch.async([
      fn ->
        Process.flag(:trap_exit, true)
        Agent.update(agent, &[1 | &1])
        Process.sleep(:infinity)
      end,
      fn ->
        Process.flag(:trap_exit, true)
        Agent.update(agent, &[2 | &1])
        Process.sleep(:infinity)
      end,
    ])

    :timer.sleep(1)
    assert [1, 2] == Agent.get(agent, fn state -> Enum.sort(state) end)

    assert Batch.shutdown(batch, :brutal_kill) == nil
    refute_received {:DOWN, _, _, _, _}

    :ok = Agent.stop(agent)
  end

  test "function/1 exits on :noconnection" do
    ref = make_ref()
    fun = Batch.function([{:timer, :sleep, [:infinity]}])
    send self(), {:DOWN, ref, self(), self(), :noconnection}
    assert catch_exit(fun.()) == {:nodedown, :nonode@nohost}
  end

  test "function/1 exits on :noconnection from named monitor" do
    ref = make_ref()
    fun = Batch.function([{:timer, :sleep, [:infinity]}])
    send self(), {:DOWN, ref, self(), {:name, :node}, :noconnection}
    assert catch_exit(fun.()) == {:nodedown, :node}
  end
end
