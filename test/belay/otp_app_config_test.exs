defmodule Belay.OtpAppConfigTest do
  use ExUnit.Case, async: true

  alias Belay.Config

  test "otp_app reads config from application env, keyed by name" do
    Application.put_env(:belay_cfg_test, MyApp.Cap,
      storage: [adapter: :memory],
      queues: [default: 10, mailers: [limit: 20]],
      poll_interval: 250
    )

    config = Config.new(otp_app: :belay_cfg_test, name: MyApp.Cap)

    assert config.name == MyApp.Cap
    assert config.poll_interval == 250
    assert config.queues["mailers"].local_limit == 20
  after
    Application.delete_env(:belay_cfg_test, MyApp.Cap)
  end

  test "inline opts override the application-env base (runtime values win)" do
    Application.put_env(:belay_cfg_test, MyApp.Cap2,
      storage: [adapter: :postgres, url: "postgres://placeholder/db"],
      poll_interval: 500
    )

    # Supervision tree passes a runtime-computed storage + a test override.
    config =
      Config.new(
        otp_app: :belay_cfg_test,
        name: MyApp.Cap2,
        storage: [adapter: :memory],
        poll_interval: 100
      )

    assert config.poll_interval == 100
    assert {Belay.Storage.Memory, _} = config.storage
  after
    Application.delete_env(:belay_cfg_test, MyApp.Cap2)
  end

  test "without otp_app the inline form is unchanged" do
    config = Config.new(name: MyApp.Cap3, storage: [adapter: :memory], queues: [q: 5])

    assert config.queues["q"].local_limit == 5
  end
end
