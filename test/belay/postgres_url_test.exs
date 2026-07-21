defmodule Belay.PostgresURLTest do
  use ExUnit.Case, async: true

  alias Belay.Storage.Postgres

  test "decodes credentials and database names and maps connection query options" do
    opts =
      Postgres.parse_url(
        "postgresql://alice%40acme:p%3Aa%2Fss@[::1]:5544/my%20db" <>
          "?sslmode=require&connect_timeout=8000&application_name=belay"
      )

    assert opts[:hostname] == "::1"
    assert opts[:port] == 5544
    assert opts[:username] == "alice@acme"
    assert opts[:password] == "p:a/ss"
    assert opts[:database] == "my db"
    # require = encrypt without verification (libpq-faithful, non-breaking).
    assert opts[:ssl] == [verify: :verify_none]
    assert opts[:connect_timeout] == 8_000
    assert opts[:parameters][:application_name] == "belay"
  end

  test "sslmode verification levels map to verified vs unverified TLS" do
    assert Postgres.parse_url("postgres://localhost/db?sslmode=verify-full")[:ssl] == true
    assert Postgres.parse_url("postgres://localhost/db?sslmode=verify-ca")[:ssl] == true

    assert Postgres.parse_url("postgres://localhost/db?sslmode=require")[:ssl] == [
             verify: :verify_none
           ]

    assert Postgres.parse_url("postgres://localhost/db?sslmode=disable")[:ssl] == false
  end

  test "supports a percent-encoded unix socket directory" do
    opts = Postgres.parse_url("postgres:///jobs?host=%2Fvar%2Frun%2Fpostgresql")

    assert opts[:socket_dir] == "/var/run/postgresql"
    refute Keyword.has_key?(opts, :hostname)
    assert opts[:database] == "jobs"
  end

  test "explicit storage options override URL-derived options" do
    custom_ssl = [verify: :verify_none]

    %{start: {Postgrex, :start_link, [opts]}} =
      Postgres.child_spec(
        {%{name: Belay.PostgresURLTest.Instance},
         url: "postgres://localhost/jobs?sslmode=require&pool_size=4",
         ssl: custom_ssl,
         pool_size: 7}
      )

    assert opts[:ssl] == custom_ssl
    assert opts[:pool_size] == 7
    assert opts[:name] == Postgres.ref(Belay.PostgresURLTest.Instance)
  end

  test "rejects unsupported schemes, fallback SSL modes, and invalid integers" do
    assert_raise ArgumentError, ~r/expected a postgres/, fn ->
      Postgres.parse_url("mysql://localhost/jobs")
    end

    assert_raise ArgumentError, ~r/fallback negotiation/, fn ->
      Postgres.parse_url("postgres://localhost/jobs?sslmode=prefer")
    end

    assert_raise ArgumentError, ~r/connect_timeout must be a positive integer/, fn ->
      Postgres.parse_url("postgres://localhost/jobs?connect_timeout=never")
    end
  end
end
