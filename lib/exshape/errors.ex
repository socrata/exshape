defmodule Exshape.Errors do
  defmodule MismatchedRecordCounts do
    defstruct []
  end

  defmodule DbfParseError do
    defstruct []
  end

  defmodule DbfRecordCountMismatch do
    @enforce_keys [:expected, :got]
    defstruct @enforce_keys
  end

  defmodule ShpParseError do
    defstruct []
  end
end
