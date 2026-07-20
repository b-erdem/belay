defmodule Capstan.Codec do
  @moduledoc """
  The cross-language value envelope for step values and job results
  (SCHEMA.md, "Value encoding").

  Binary values in `capstan_steps.value` and `capstan_jobs.result` are one of:

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
  def decode(<<@etf_tag, _rest::binary>> = binary), do: :erlang.binary_to_term(binary)
  def decode(binary) when is_binary(binary), do: Jason.decode!(binary)
end
