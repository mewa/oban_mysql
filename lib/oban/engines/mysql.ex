defmodule Oban.Engines.MySQL do
  @moduledoc """
  An engine for running Oban with MySQL.

  ## Usage

  Start an `Oban` instance using the `MySQL` engine:


      Oban.start_link(
        engine: Oban.Engines.MySQL,
        queues: [default: 10],
        repo: MyApp.Repo
      )
  """

  @behaviour Oban.Engine

  import Ecto.Query
  import DateTime, only: [utc_now: 0]

  alias Oban.Engines.Basic
  alias Oban.Engine

  alias Ecto.Changeset
  alias Oban.{Config, Engine, Job, Repo}

  defmacrop json_push(column, value) do
    quote do
      fragment("json_array_append(?, '$', ?)", unquote(column), unquote(value))
    end
  end

  defmacrop json_contains(column, object) do
    quote do
      fragment(
        """
        json_contains(?, ?, '$')
        """,
        unquote(column),
        unquote(object)
      )
    end
  end

  @impl Engine
  defdelegate init(conf, opts), to: Basic

  @impl Engine
  defdelegate put_meta(conf, meta, key, value), to: Basic

  @impl Engine
  defdelegate check_meta(conf, meta, running), to: Basic

  @impl Engine
  defdelegate refresh(conf, meta), to: Basic

  @impl Engine
  defdelegate shutdown(conf, meta), to: Basic

  @impl Engine
  def insert_job(conf, changeset, opts) do
    insert_unique(conf, changeset, opts)
  end

  @impl Engine
  defdelegate insert_all_jobs(conf, changesets, opts), to: Basic

  @impl Engine
  def fetch_jobs(_conf, %{paused: true} = meta, _running) do
    {:ok, {meta, []}}
  end

  def fetch_jobs(_conf, %{limit: limit} = meta, running) when map_size(running) >= limit do
    {:ok, {meta, []}}
  end

  def fetch_jobs(%Config{} = conf, meta, running) do
    demand = meta.limit - map_size(running)

    subset =
      Job
      |> where([j], j.state == "available")
      |> where([j], j.queue == ^meta.queue)
      |> where([j], j.attempt < j.max_attempts)
      |> order_by([j], asc: j.priority, asc: j.scheduled_at, asc: j.id)
      |> limit(^demand)

    updates = [
      set: [state: "executing", attempted_at: utc_now(), attempted_by: [meta.node]],
      inc: [attempt: 1]
    ]

    Repo.transaction(conf, fn ->
      jobs = Repo.all(conf, subset)
      job_ids = jobs |> Enum.map(&(&1.id))

      query =
        Job
        |> where([j], j.id in ^job_ids)

      case Repo.update_all(conf, query, updates) do
        {0, nil} -> []
        {_count, jobs} -> jobs
      end

      {meta, Repo.all(conf, query)}
    end)
  end
  
  @impl Engine
  def stage_jobs(conf, queryable, opts) do
    limit = Keyword.fetch!(opts, :limit)

    subquery =
      queryable
      |> select([:id, :state])
      |> where([j], j.state in ~w(scheduled retryable))
      |> where([j], not is_nil(j.queue))
      |> where([j], j.priority in [0, 1, 2, 3])
      |> where([j], j.scheduled_at <= ^DateTime.utc_now())
      |> limit(^limit)

    query =
      Job
      |> join(:inner, [j], x in subquery(subquery), on: j.id == x.id)
      |> select([j, x], %{id: j.id, queue: j.queue, state: x.state})

    Repo.transaction(conf, fn ->
      staged = Repo.all(conf, query)
      job_ids = staged |> Enum.map(&(&1.id))

      query = Job
      |> where([j], j.id in ^job_ids)

      {_count, _} = Repo.update_all(conf, query, set: [state: "available"])

      Repo.all(conf, query)
    end)
  end

  @impl Engine
  defdelegate prune_jobs(conf, queryable, opts), to: Basic

  @impl Engine
  defdelegate complete_job(conf, job), to: Basic

  @impl Engine
  def discard_job(conf, job) do
    query =
      Job
      |> where(id: ^job.id)
      |> update([j],
        set: [
          state: "discarded",
          discarded_at: ^utc_now(),
          errors: json_push(j.errors, ^encode_unsaved(job))
        ]
      )

    Repo.update_all(conf, query, [])

    :ok
  end

  @impl Engine
  def error_job(%Config{} = conf, job, seconds) do
    query =
      Job
      |> where(id: ^job.id)
      |> update([j],
        set: [
          state: "retryable",
          scheduled_at: ^seconds_from_now(seconds),
          errors: json_push(j.errors, ^encode_unsaved(job))
        ]
      )

    Repo.update_all(conf, query, [])

    :ok
  end

  @impl Engine
  defdelegate snooze_job(conf, job, seconds), to: Basic

  @impl Engine
  def cancel_job(conf, job) do
    query = where(Job, id: ^job.id)

    query =
      if is_map(job.unsaved_error) do
        update(query, [j],
          set: [
            state: "cancelled",
            cancelled_at: ^utc_now(),
            errors: json_push(j.errors, ^encode_unsaved(job))
          ]
        )
      else
        query
        |> where([j], j.state not in ["cancelled", "completed", "discarded"])
        |> update(set: [state: "cancelled", cancelled_at: ^utc_now()])
      end

    Repo.update_all(conf, query, [])

    :ok
  end

  @impl Engine
  defdelegate cancel_all_jobs(conf, queryable), to: Basic

  @impl Engine
  defdelegate retry_job(conf, job), to: Basic

  @impl Engine
  defdelegate retry_all_jobs(conf, queryable), to: Basic

  # Insertion

  defp insert_unique(conf, changeset, opts) do
    opts = Keyword.put(opts, :on_conflict, :nothing)

    lock_info = unique_query(changeset)
    try do
      {:ok, result} = Repo.transaction(conf, fn ->
        with {:ok, query, lock_key} <- lock_info,
             :ok <- acquire_lock(conf, lock_key),
             {:ok, job} <- fetch_job(conf, query, opts),
             {:ok, job} <- resolve_conflict(conf, job, changeset, opts) do

          {:ok, %Job{job | conflict?: true}}
        else
          {:error, :locked} ->
            Changeset.apply_action(changeset, :insert)

          nil ->
            Repo.insert(conf, changeset, opts)

          error ->
            error
        end

      end)

      result
    after
      with {:ok, _, lock_key} <- lock_info do
        release_lock(conf, lock_key)
      end
    end
    
  end

  defp resolve_conflict(conf, job, changeset, opts) do
    case Changeset.fetch_change(changeset, :replace) do
      {:ok, replace} ->
        keys = Keyword.get(replace, String.to_existing_atom(job.state), [])

        Repo.update(
          conf,
          Changeset.change(job, Map.take(changeset.changes, keys)),
          opts
        )

      :error ->
        {:ok, job}
    end
  rescue
    error in [Ecto.StaleEntryError] ->
      {:error, error}
  end

  defp unique_query(%{changes: %{unique: %{} = unique}} = changeset) do
    %{fields: fields, keys: keys, period: period, states: states, timestamp: timestamp} = unique

    keys = Enum.map(keys, &to_string/1)
    states = Enum.map(states, &to_string/1)
    dynamic = Enum.reduce(fields, true, &unique_field({changeset, &1, keys}, &2))
    lock_key = :erlang.phash2([keys, states, dynamic])

    query =
      Job
      |> where([j], j.state in ^states)
      |> since_period(period, timestamp)
      |> where(^dynamic)
      |> limit(1)

    {:ok, query, lock_key}
  end

  defp unique_query(_changeset), do: nil

  defp unique_field({changeset, field, keys}, acc) when field in [:args, :meta] do
    value = unique_map_values(changeset, field, keys)

    cond do
      value == %{} ->
        dynamic([j], json_contains(^value, field(j, ^field)) and ^acc)

      keys == [] ->
        dynamic(
          [j],
          json_contains(field(j, ^field), ^value) and
            json_contains(^value, field(j, ^field)) and ^acc
        )

      true ->
        dynamic([j], json_contains(field(j, ^field), ^value) and ^acc)
    end
  end

  defp unique_field({changeset, field, _}, acc) do
    value = Changeset.get_field(changeset, field)

    dynamic([j], field(j, ^field) == ^value and ^acc)
  end

  defp unique_map_values(changeset, field, keys) do
    case keys do
      [] ->
        Changeset.get_field(changeset, field)

      [_ | _] ->
        changeset
        |> Changeset.get_field(field)
        |> Map.new(fn {key, val} -> {to_string(key), val} end)
        |> Map.take(keys)
    end
  end

  defp since_period(query, :infinity, _timestamp), do: query

  defp since_period(query, period, timestamp) do
    where(query, [j], field(j, ^timestamp) >= ^seconds_from_now(-period))
  end

  defp acquire_lock(%{testing: :disabled} = conf, base_key) do
    pref_key = :erlang.phash2(conf.prefix)
    lock_key = "oban_#{pref_key + base_key}"

    case Repo.query(conf, "SELECT GET_LOCK(?, ?)", [lock_key, 0]) do
      {:ok, %{rows: [[1]]}} -> :ok
      _ -> {:error, :locked}
    end
  end

  defp acquire_lock(_conf, _key), do: :ok

  defp release_lock(conf, base_key) do
    pref_key = :erlang.phash2(conf.prefix)
    lock_key = "oban_#{pref_key + base_key}"

    Repo.query(conf, "SELECT RELEASE_LOCK(?)", [lock_key])
  end

  defp fetch_job(conf, query, opts) do
    case Repo.one(conf, query, Keyword.put(opts, :prepare, :unnamed)) do
      nil -> nil
      job -> {:ok, job}
    end
  end

  defp seconds_from_now(seconds), do: DateTime.add(utc_now(), seconds, :second)

  defp encode_unsaved(job) do
    job
    |> Job.format_attempt()
    |> Jason.encode!()
  end
end
