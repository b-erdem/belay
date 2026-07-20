defmodule Capstan.Query do
  @moduledoc false

  # Dialect-portable query fragments over `oban_jobs.meta`. Every helper takes the
  # adapter tag (`:pg` | :sqlite`) so call sites stay free of fragment branching.

  import Ecto.Query

  alias Oban.Config

  def dialect(%Config{repo: repo}), do: dialect(repo)

  def dialect(repo) when is_atom(repo) do
    case repo.__adapter__() do
      Ecto.Adapters.SQLite3 -> :sqlite
      _ -> :pg
    end
  end

  @doc "meta->>key = value"
  def meta_eq(:pg, key, value) do
    dynamic([j], fragment("?->>? = ?", j.meta, ^key, ^to_string(value)))
  end

  def meta_eq(:sqlite, key, value) do
    dynamic([j], fragment("json_extract(?, ?) = ?", j.meta, ^"$.#{key}", ^to_string(value)))
  end

  @doc "meta->'workflow_deps' contains name"
  def deps_contain(:pg, name) do
    dynamic(
      [j],
      fragment(
        "EXISTS (SELECT 1 FROM jsonb_array_elements_text(?->'workflow_deps') AS e(v) WHERE e.v = ?)",
        j.meta,
        ^name
      )
    )
  end

  def deps_contain(:sqlite, name) do
    dynamic(
      [j],
      fragment(
        "EXISTS (SELECT 1 FROM json_each(json_extract(?, '$.workflow_deps')) WHERE json_each.value = ?)",
        j.meta,
        ^name
      )
    )
  end

  @doc "Select the value at meta->>key"
  def meta_value(:pg, key) do
    dynamic([j], fragment("?->>?", j.meta, ^key))
  end

  def meta_value(:sqlite, key) do
    dynamic([j], fragment("json_extract(?, ?)", j.meta, ^"$.#{key}"))
  end
end
