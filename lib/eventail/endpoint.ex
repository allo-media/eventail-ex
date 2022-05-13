defmodule Eventail.Endpoint do
  @enforce_keys [:logical_service]
  defstruct [:logical_service, hostname: "", log: nil, business: nil, conf: nil]

  @type t :: %__MODULE__{
          logical_service: atom,
          hostname: String.t(),
          log: pid(),
          business: pid(),
          conf: pid()
        }
end
