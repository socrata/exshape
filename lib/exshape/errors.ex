defmodule Exshape.Errors do
  defmodule MismatchedRecordCounts do
    defexception message: "Mismatched DBF/SHP record counts"
  end

  defmodule DbfParseError do
    defexception message: "DBF parse error"
  end

  defmodule DbfRecordCountMismatch do
    defexception [:expected, :got, {:message, "DBF record count mismatch"}]
  end

  defmodule ShpParseError do
    defexception message: "SHP parse error"
  end
end
