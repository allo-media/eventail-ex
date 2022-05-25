defmodule Eventail.Endpoint do
  @doc """
  The endpoint descriptor struct.
  """

  @enforce_keys [:logical_service, :pid]
  defstruct [:logical_service, hostname: "", pid: nil]

  @type t :: %__MODULE__{
          logical_service: atom,
          hostname: String.t(),
          pid: pid()
        }
end
