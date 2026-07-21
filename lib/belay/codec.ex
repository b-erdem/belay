defmodule Belay.Codec do
  @moduledoc """
  The cross-language value envelope for step values and job results
  (SCHEMA.md, "Value encoding").

  Binary values in `belay_steps.value` and `belay_jobs.result` are one of:

    * **Erlang external term format** — first byte `131`; written by the
      Elixir SDK (`:erlang.term_to_binary/1`), which preserves rich terms.
    * **UTF-8 JSON** — anything else; the required encoding for non-Elixir
      SDKs. JSON never begins with byte `131`, so the discriminator is exact.

  Every SDK must *read* both; each SDK *writes* its native side. Reserved
  step values (`$sleep:*`, `$spawn:*`) are SDK-internal — treat foreign ones
  as opaque.
  """

  @etf_tag 131

  @doc "Encode a term for storage (Elixir writes ETF)."
  def encode(term), do: :erlang.term_to_binary(term)

  @doc "Decode a stored value: ETF or JSON by the leading-byte discriminator."
  def decode(nil), do: nil
  # `:safe` blocks new-atom creation and other unsafe constructs, so a
  # crafted ETF payload in these columns (they may be written by a foreign
  # SDK) can't exhaust the atom table or build dangerous terms. Values are
  # data by contract — no pids/refs/funs — so this rejects nothing legitimate.
  def decode(<<@etf_tag, _rest::binary>> = binary), do: :erlang.binary_to_term(binary, [:safe])
  def decode(binary) when is_binary(binary), do: Jason.decode!(binary)
end
