defmodule Exshape.Shp do
  require Rustler
  use Rustler, otp_app: :exshape, crate: :exshape_shape, mode: :release

  alias Exshape.Errors

  defmodule State do
    @enforce_keys [:nest_polygon, :raise_on_nan_points]
    defstruct mode: :header,
      shape_type: nil,
      emit: [],
      to_read: nil,
      item: nil,
      part_index: 0,
      measures: [],
      z_values: [],
      nest_polygon: nil,
      raise_on_nan_points: nil,
      remaining: :infinity
  end

  @magic_nodata_num :math.pow(10, 38) * -1

  defmodule Bbox do
    defstruct [:xmin, :xmax, :ymin, :ymax, :zmin, :zmax, :mmin, :mmax]
  end

  defmodule Header do
    defstruct [:bbox, :shape_type]
  end

  defmodule Point do
    defstruct [:x, :y]
  end

  defmodule PointM do
    defstruct [:x, :y, :m]
  end

  defmodule PointZ do
    defstruct [:x, :y, :m, :z]
  end

  defmodule Multipoint do
    defstruct points: [], bbox: nil
  end

  defmodule MultipointM do
    defstruct points: [], bbox: nil
  end

  defmodule MultipointZ do
    defstruct points: [], bbox: nil
  end

  defmodule Polyline do
    defstruct points: [], bbox: nil, parts: []
  end

  defmodule PolylineM do
    defstruct points: [], bbox: nil, parts: []
  end

  defmodule PolylineZ do
    defstruct points: [], bbox: nil, parts: []
  end

  defmodule PolygonM do
    defstruct points: [], bbox: nil, parts: []
  end

  defmodule PolygonZ do
    defstruct points: [], bbox: nil, parts: []
  end

  defmodule Polygon do
    defstruct points: [], bbox: nil, parts: []
  end


  @file_code <<9994::big-integer-size(32)>>
  @unused <<0::big-integer-size(32)>>
  @version <<1000::little-integer-size(32)>>

  Enum.each([{0, nil, nil},
  {1, :point, Point},
  {3, :polyline, Polyline},
  {5, :polygon, Polygon},
  {8, :multipoint, Multipoint},
  {11, :pointz, PointZ},
  {13, :polylinez, PolylineZ},
  {15, :polygonz, PolygonZ},
  {18, :multipointz, MultipointZ},
  {21, :pointm, PointM},
  {23, :polylinem, PolylineM},
  {25, :polygonm, PolygonM},
  {28, :multipointm, MultipointM},
  {31, :multipatchm, nil}], fn {code, t, s} ->
    def shape_type_from_code(unquote(code)), do: unquote(t)
    def shape_type_to_struct(unquote(t)), do: struct!(unquote(s))
  end)

  defmacrop dbg(_msg) do
    quote do
      # IO.puts(unquote(msg))
      :ok
    end
  end

  defp zip_measures(p, %{measures: []}) do
    p
  end
  defp zip_measures(p, s) do
    points = p.points
    |> Enum.zip(s.measures)
    |> Enum.map(fn {pm, m} -> %{pm | m: m} end)

    %{p | points: points}
  end

  defp zip_zvals(p, s) do
    points = p.points
    |> Enum.zip(s.z_values)
    |> Enum.map(fn {pm, z} -> %{pm | z: z} end)

    %{p | points: points}
  end

  defp reset_unzipped(s) do
    %{s | measures: [], z_values: []}
  end

  defp emit(%State{nest_polygon: nest_polygon} = s, %Polygon{} = p) do
    %{s | mode: :record_header, emit: [%{p | points: nest_polygon.(p)} | s.emit]}
  end

  defp emit(s, %Polyline{} = p) do
    %{s | mode: :record_header, emit: [%{p | points: unflatten_parts(p)} | s.emit]}
  end

  defp emit(s, %Multipoint{} = mp) do
    %{s | mode: :record_header, emit: [reverse(mp, :points) | s.emit]}
  end

  defp emit(s, %MultipointM{} = mp) do
    mp = zip_measures(mp, s) |> reverse(:points)
    %{s | mode: :record_header, emit: [mp | s.emit]} |> reset_unzipped
  end

  defp emit(s, %PolylineM{} = pm) do
    p = zip_measures(pm, s)
    polylinem = %{p | points: unflatten_parts(p)}
    %{s | mode: :record_header, emit: [polylinem | s.emit]} |> reset_unzipped
  end

  defp emit(%State{nest_polygon: nest_polygon} = s, %PolygonM{} = pm) do
    p = zip_measures(pm, s)
    polylinem = %{p | points: nest_polygon.(p)}
    %{s | mode: :record_header, emit: [polylinem | s.emit]} |> reset_unzipped
  end

  defp emit(s, %MultipointZ{} = mp) do
    mp = mp
    |> zip_measures(s)
    |> zip_zvals(s)
    |> reverse(:points)

    %{s | mode: :record_header, emit: [mp | s.emit]} |> reset_unzipped
  end

  defp emit(s, %PolylineZ{} = pz) do
    p = pz
    |> zip_measures(s)
    |> zip_zvals(s)

    polylinez = %{p | points: unflatten_parts(p)}
    %{s | mode: :record_header, emit: [polylinez | s.emit]} |> reset_unzipped
  end

  defp emit(%State{nest_polygon: nest_polygon} = s, %PolygonZ{} = pz) do
    p = pz
    |> zip_measures(s)
    |> zip_zvals(s)

    polygonz = %{p | points: nest_polygon.(p)}
    %{s | mode: :record_header, emit: [polygonz | s.emit]} |> reset_unzipped
  end

  defp emit(s, thing), do: %{s | mode: :record_header, emit: [thing | s.emit], item: nil}

  defguardp available(s, bits, rest) when s.remaining >= byte_size(bits) - byte_size(rest)

  defp mode(s, m, cl), do: %{s | mode: m, remaining: cl*2}
  defp mode(s, m), do: %{s | mode: m}
  defp take(%{remaining: remaining} = s, bits, rest), do: %{s | remaining: remaining - (byte_size(bits)-byte_size(rest))}
  defp shape_type(s, st), do: %{s | shape_type: st}
  defp item(s, item), do: %{s | item: item}
  defp repeatedly(s, n), do: %{s | to_read: n}
  defp prepend(s, p, key) do
    %{s | item: Map.put(s.item, key, [p | Map.get(s.item, key)])}
  end
  defp consume_item(s), do: %{s | to_read: s.to_read - 1}
  defp emit_item(s), do: %{emit(s, s.item) | item: nil}
  defp reverse(item, key) do
    Map.put(item, key, Enum.reverse(Map.get(item, key)))
  end

  defp nodata_to_nil(n) when n < @magic_nodata_num, do: nil
  defp nodata_to_nil(n), do: n

  defp put_measure(s, m), do: %{s | measures: [m | s.measures]}

  defp put_z(s, z), do: %{s | z_values: [z | s.z_values]}


  defp unflatten_parts(item) do
    parts_map = MapSet.new(item.parts)

    count = length(item.points) - 1
    # Moving backwards through this list allows us to do fewer reverse calls,
    # but it is more confusing
    {parts, _} = item.points
    |> Enum.reduce({[], count}, fn
      point, {nested, 0} ->
        [nest | rest_nested] = nested
        {[[point | nest] | rest_nested], 0}
      point, {[], ^count} ->
        {[[point]], count - 1}
      point, {nested, i} ->
        [nest | rest_nested] = nested
        new_nest = [point | nest]

        if MapSet.member?(parts_map, i) do
          {[[], new_nest | rest_nested], i - 1}
        else
          {[new_nest | rest_nested], i - 1}
        end
    end)

    parts
  end

  def native_nest_polygon(p) do
    {:ok, r} = native_nest_polygon_impl(unflatten_parts(p))
    r
  end
  defp native_nest_polygon_impl(_p), do: throw :nif_not_loaded

  def beam_nest_polygon(p) do
    {polys, holes} = unflatten_parts(p) |> Enum.split_with(&is_clockwise?/1)

    Enum.reduce(holes, Enum.map(polys, fn p -> [p] end), fn hole, polys ->
      nest_hole(hole, polys)
    end)
  end

  def nest_hole(hole, []), do: [[hole]]
  # Optimization for the most common case: if there's only one exterior ring,
  # the hole gets put in it, rather than verifying that it contains it (the spec
  # says that it must contain it, otherwise there would be holes in empty space)
  def nest_hole(hole, [poly]), do: [poly ++ [hole]]
  def nest_hole([point | _] = hole, [[first_ring | _] = poly | rest_polys]) do
    if ring_contains?(first_ring, point) do
      [poly ++ [hole] | rest_polys]
    else
      [poly | nest_hole(hole, rest_polys)]
    end
  end

  def is_clockwise?(points) when length(points) < 4, do: false
  def is_clockwise?([prev | points]) do
    {_, area} = Enum.reduce(points, {prev, 0}, fn %{x: x, y: y} = np, {%{x: xp, y: yp}, s} ->
      {np, s + (x - xp) * (y + yp)}
    end)

    area >= 0
  end

  def ring_contains?([], _), do: false
  def ring_contains?(ring, %{x: x, y: y}) do
    {_, c} = Enum.reduce(ring, {List.last(ring), false}, fn %{x: ix, y: iy} = i, {%{x: jx, y: jy}, c} ->
      c = if ((iy > y) != (jy > y)) && (x < ((((jx - ix) * (y - iy)) / (jy - iy)) + ix)) do
        !c
      else
        c
      end

      {i, c}
    end)

    c
  end

  defp extract_bbox(<<
    xmin::little-float-size(64),
    ymin::little-float-size(64),
    xmax::little-float-size(64),
    ymax::little-float-size(64)
  >>, zmin, zmax) do
    %Bbox{xmin: xmin, ymin: ymin, xmax: xmax, ymax: ymax, zmin: zmin, zmax: zmax}
  end
  defp extract_bbox(_, _, _), do: %Bbox{}
  defp extract_bbox(v), do: extract_bbox(v, nil, nil)

  defp update_bbox_zrange(bbox, <<
    zmin::little-float-size(64),
    zmax::little-float-size(64),
  >>) do
    %{bbox | zmin: zmin, zmax: zmax}
  end
  defp update_bbox_zrange(bbox, _), do: bbox


  defp update_bbox_measures(bbox, <<
    mmin::little-float-size(64),
    mmax::little-float-size(64),
  >>) do
    %{bbox | mmin: mmin, mmax: mmax}
  end
  defp update_bbox_measures(bbox, _), do: bbox

  defp do_read(%State{mode: :header} = s, <<
    @file_code,
    @unused,
    @unused,
    @unused,
    @unused,
    @unused,
    _file_len::big-integer-size(32),
    @version,
    type_code::little-integer-size(32),
    xmin::little-float-size(64),
    ymin::little-float-size(64),
    xmax::little-float-size(64),
    ymax::little-float-size(64),
    zmin::little-float-size(64),
    zmax::little-float-size(64),
    mmin::little-float-size(64),
    mmax::little-float-size(64),
    rest::binary
  >>) do
    dbg("header")
    box = %Bbox{
      xmin: xmin,
      xmax: xmax,
      ymin: ymin,
      ymax: ymax,
      zmin: zmin,
      zmax: zmax,
      mmin: mmin,
      mmax: mmax
    }
    st = shape_type_from_code(type_code)

    s
    |> emit(%Header{bbox: box, shape_type: st})
    |> shape_type(st)
    |> do_read(rest)
  end

  defp do_read(%State{mode: :record_header} = s, <<
    record_number::big-integer-size(32),
    content_length::big-integer-size(32),
    rest::binary
  >>) do
    dbg("record_header #{record_number} #{content_length}")
    s
    |> mode({:record, record_number}, content_length)
    |> do_read(rest)
  end

  defp do_read(%State{mode: {:record, _}, shape_type: _} = s, <<
    0::little-integer-size(32),
    rest::binary
  >> = bits) do
    dbg("record/nil #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> emit(nil)
    |> do_read(rest)
  end


  ##
  # Point
  #
  defp do_read(%State{mode: {:record, _}, shape_type: :point} = s, <<
    1::little-integer-size(32),
    x::little-float-size(64),
    y::little-float-size(64),
    rest::binary
  >> = bits) do
    dbg("record/point #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> emit(%Point{x: x, y: y})
    |> do_read(rest)
  end
  defp do_read(%State{mode: {:record, _}, shape_type: :point, raise_on_nan_points: e} = s, <<
    1::little-integer-size(32),
    _::binary-size(16),
    rest::binary
  >> = bits) when e in [false, nil] do
    dbg("record/nan point #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> emit(nil)
    |> do_read(rest)
  end

  ##
  # Multipoints
  #
  defp do_read(%State{mode: {:record, _}, shape_type: :multipoint} = s, <<
    8::little-integer-size(32),
    bbox::binary-size(32),
    num_points::little-integer-size(32),
    rest::binary
  >> = bits) do
    dbg("record/multipoint #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> repeatedly(num_points)
    |> item(%Multipoint{bbox: extract_bbox(bbox)})
    |> mode(:multipoint)
    |> do_read(rest)
  end

  defp do_read(%State{mode: :multipoint, to_read: 0} = s, rest) do
    dbg("multipoint complete")
    s
    |> emit_item
    |> do_read(rest)
  end

  defp do_read(%State{mode: :multipoint, shape_type: :multipoint} = s, <<
    x::little-float-size(64),
    y::little-float-size(64),
    rest::binary
  >> = bits) do
    dbg("multipoint/multipoint #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> prepend(%Point{x: x, y: y}, :points)
    |> consume_item
    |> do_read(rest)
  end

  ##
  # Polylines
  #
  defp do_read(%State{mode: {:record, _}, shape_type: :polyline} = s, <<
    3::little-integer-size(32),
    bbox::binary-size(32),
    num_parts::little-integer-size(32),
    num_points::little-integer-size(32),
    rest::binary
  >> = bits) do
    dbg("record/polyline #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> repeatedly(num_parts)
    |> item(%Polyline{bbox: extract_bbox(bbox)})
    |> mode({:parts, {:polyline, num_points}})
    |> do_read(rest)
  end

  defp do_read(%State{mode: :polyline, to_read: 0} = s, rest) do
    dbg("polyline complete")
    s
    |> emit(s.item)
    |> do_read(rest)
  end

  defp do_read(%State{mode: :polyline, shape_type: :polyline} = s, <<
    x::little-float-size(64),
    y::little-float-size(64),
    rest::binary
  >> = bits) do
    dbg("polyline/polyline #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> prepend(%Point{x: x, y: y}, :points)
    |> consume_item
    |> do_read(rest)
  end

  ##
  # Polygons
  #
  defp do_read(%State{mode: {:record, _}, shape_type: :polygon} = s, <<
    5::little-integer-size(32),
    bbox::binary-size(32),
    num_parts::little-integer-size(32),
    num_points::little-integer-size(32),
    rest::binary
  >> = bits) do
    dbg("record/polygon #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> repeatedly(num_parts)
    |> item(%Polygon{bbox: extract_bbox(bbox)})
    |> mode({:parts, {:polygon, num_points}})
    |> do_read(rest)
  end

  defp do_read(%State{mode: :polygon, to_read: 0} = s, rest) do
    dbg("polygon complete")
    s
    |> emit(s.item)
    |> do_read(rest)
  end

  defp do_read(%State{mode: :polygon, shape_type: :polygon} = s, <<
    x::little-float-size(64),
    y::little-float-size(64),
    rest::binary
  >> = bits) do
    dbg("polygon/polygon #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> prepend(%Point{x: x, y: y}, :points)
    |> consume_item
    |> do_read(rest)
  end

  ##
  # PointM
  #
  defp do_read(%State{mode: {:record, _}, shape_type: :pointm} = s, <<
    21::little-integer-size(32),
    x::little-float-size(64),
    y::little-float-size(64),
    m::little-float-size(64),
    rest::binary
  >> = bits) do
    dbg("record/pointm #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> emit(%PointM{x: x, y: y, m: m})
    |> do_read(rest)
  end

  ##
  # PointZ
  #
  defp do_read(%State{mode: {:record, _}, shape_type: :pointz} = s, <<
    11::little-integer-size(32),
    x::little-float-size(64),
    y::little-float-size(64),
    z::little-float-size(64),
    m::little-float-size(64),
    rest::binary
  >> = bits) do
    dbg("record/pointz #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> emit(%PointZ{x: x, y: y, m: nodata_to_nil(m), z: z})
    |> do_read(rest)
  end

  ##
  # MultipointM
  #
  defp do_read(%State{mode: {:record, _}, shape_type: :multipointm} = s, <<
    28::little-integer-size(32),
    bbox::binary-size(32),
    num_points::little-integer-size(32),
    rest::binary
  >> = bits) do
    dbg("record/multipointm #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> repeatedly(num_points)
    |> item(%MultipointM{bbox: extract_bbox(bbox)})
    |> mode(:multipointm)
    |> do_read(rest)
  end


  defp do_read(%State{mode: :multipointm, to_read: 0} = s, rest) do
    dbg("multipointm complete")
    s
    |> mode(:m)
    |> do_read(rest)
  end

  defp do_read(%State{mode: :multipointm, shape_type: :multipointm} = s, <<
    x::little-float-size(64),
    y::little-float-size(64),
    rest::binary
  >> = bits) do
    dbg("multipointm/multipointm #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> prepend(%PointM{x: x, y: y}, :points)
    |> consume_item
    |> do_read(rest)
  end

  ##
  # MultipointZ
  #
  defp do_read(%State{mode: {:record, _}, shape_type: :multipointz} = s, <<
    18::little-integer-size(32),
    bbox::binary-size(32),
    num_points::little-integer-size(32),
    rest::binary
  >> = bits) do
    dbg("record/multipointz #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> repeatedly(num_points)
    |> item(%MultipointZ{bbox: extract_bbox(bbox)})
    |> mode(:multipointz)
    |> do_read(rest)
  end


  defp do_read(%State{mode: :multipointz, to_read: 0} = s, rest) do
    dbg("multipointz complete")
    s
    |> mode(:z)
    |> do_read(rest)
  end

  defp do_read(%State{mode: :multipointz, shape_type: :multipointz} = s, <<
    x::little-float-size(64),
    y::little-float-size(64),
    rest::binary
  >> = bits) do
    dbg("multipointz/multipointz #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> prepend(%PointZ{x: x, y: y}, :points)
    |> consume_item
    |> do_read(rest)
  end

  ##
  # PolylineM and PolygonM are the same
  @poly_m [:polylinem, :polygonm]
  @poly_m_t %{
    polylinem: PolylineM,
    polygonm: PolygonM
  }

  defp do_read(%State{mode: {:record, _}, shape_type: st} = s, <<
    _::little-integer-size(32),
    bbox::binary-size(32),
    num_parts::little-integer-size(32),
    num_points::little-integer-size(32),
    rest::binary
  >> = bits) when st in @poly_m do

    dbg("record/#{inspect st} #{byte_size(bits) - byte_size(rest)}")
    t = Map.get(@poly_m_t, st)
    item = struct(t, %{bbox: extract_bbox(bbox)})

    s
    |> take(bits, rest)
    |> repeatedly(num_parts)
    |> item(item)
    |> mode({:parts, {st, num_points}})
    |> do_read(rest)
  end

  defp do_read(%State{mode: mode, to_read: 0} = s, rest) when mode in @poly_m do
    dbg("#{inspect mode} complete")
    s
    |> mode(:m)
    |> do_read(rest)
  end

  defp do_read(%State{mode: st, shape_type: st} = s, <<
    x::little-float-size(64),
    y::little-float-size(64),
    rest::binary
  >> = bits) when st in @poly_m do
    dbg("#{inspect st}/#{inspect st} #{byte_size(bits) - byte_size(rest)}")
    s
    |> take(bits, rest)
    |> prepend(%PointM{x: x, y: y}, :points)
    |> consume_item
    |> do_read(rest)
  end

  ##
  # PolylineZ and PolygonZ are the same
  @poly_z [:polylinez, :polygonz]
  @poly_z_t %{
    polylinez: PolylineZ,
    polygonz: PolygonZ
  }

  defp do_read(%State{mode: {:record, _}, shape_type: st} = s, <<
    _::little-integer-size(32),
    bbox::binary-size(32),
    num_parts::little-integer-size(32),
    num_points::little-integer-size(32),
    rest::binary
  >> = bits) when st in @poly_z do
    dbg("record/#{inspect st}/#{num_parts}/#{num_points} #{byte_size(bits) - byte_size(rest)}")

    t = Map.get(@poly_z_t, st)
    item = struct(t, %{bbox: extract_bbox(bbox)})

    s
    |> take(bits, rest)
    |> repeatedly(num_parts)
    |> item(item)
    |> mode({:parts, {st, num_points}})
    |> do_read(rest)
  end

  defp do_read(%State{mode: mode, to_read: 0} = s, rest) when mode in @poly_z do
    dbg("#{inspect mode} complete")

    s
    |> mode(:z)
    |> do_read(rest)
  end

  defp do_read(%State{mode: st, shape_type: st} = s, <<
    x::little-float-size(64),
    y::little-float-size(64),
    rest::binary
  >> = bits) when st in @poly_z do
    dbg("#{inspect st}/#{inspect st} #{byte_size(bits) - byte_size(rest)}")

    s
    |> take(bits, rest)
    |> prepend(%PointZ{x: x, y: y}, :points)
    |> consume_item
    |> do_read(rest)
  end

  ##
  # Parts
  #
  defp do_read(%State{mode: {:parts, {next_mode, to_read}}, to_read: 0} = s, rest) do
    dbg("parts/#{inspect next_mode}")

    s
    |> item(reverse(s.item, :parts))
    |> mode(next_mode)
    |> repeatedly(to_read)
    |> do_read(rest)
  end
  defp do_read(%State{mode: {:parts, _}} = s, <<
    part::little-integer-size(32),
    rest::binary
  >> = bits) do
    dbg("parts/#{part} #{byte_size(bits) - byte_size(rest)}")

    s
    |> take(bits, rest)
    |> prepend(part, :parts)
    |> consume_item
    |> do_read(rest)
  end

  ##
  # Measures
  #
  defp do_read(%State{mode: :m} = s, <<
    bbox_measures::binary-size(16),
    rest::binary
  >> = bits) when available(s,bits, rest) do
    dbg("measures #{byte_size(bits) - byte_size(rest)}")

    num_points = length(s.item.points)
    bbox = update_bbox_measures(s.item.bbox, bbox_measures)

    s
    |> take(bits, rest)
    |> mode(:measures)
    |> repeatedly(num_points)
    |> item(%{s.item | bbox: bbox})
    |> do_read(rest)
  end

  defp do_read(%State{mode: :m, remaining: 0} = s, rest) do
    dbg("No measures!")
    s
    |> mode(:measures)
    |> repeatedly(0)
    |> do_read(rest)
  end

  defp do_read(%State{mode: :measures, to_read: 0} = s, rest) do
    dbg("measures complete")

    s
    |> emit(s.item)
    |> do_read(rest)
  end

  defp do_read(%State{mode: :measures} = s, <<
    m::little-float-size(64),
    rest::binary
  >> = bits) do
    dbg("measures/measures #{byte_size(bits) - byte_size(rest)}")

    s
    |> take(bits, rest)
    |> put_measure(nodata_to_nil(m))
    |> consume_item
    |> do_read(rest)
  end

  ##
  # Z Values
  #

  defp do_read(%State{mode: :z} = s, <<
    z_range::binary-size(16),
    rest::binary
  >> = bits) do
    dbg("z #{byte_size(bits) - byte_size(rest)}")

    num_points = length(s.item.points)
    bbox = update_bbox_zrange(s.item.bbox, z_range)
    s
    |> take(bits, rest)
    |> mode(:z_values)
    |> repeatedly(num_points)
    |> item(%{s.item | bbox: bbox})
    |> do_read(rest)
  end

  defp do_read(%State{mode: :z_values, to_read: 0} = s, rest) do
    dbg("z complete")

    num_points = length(s.item.points)

    s
    |> mode(:m)
    |> repeatedly(num_points)
    |> do_read(rest)
  end

  defp do_read(%State{mode: :z_values} = s, <<
    z::little-float-size(64),
    rest::binary
  >> = bits) do
    dbg("z/z_values #{byte_size(bits) - byte_size(rest)}")

    s
    |> take(bits, rest)
    |> put_z(z)
    |> consume_item
    |> do_read(rest)
  end



  defp do_read(%State{} = s, <<rest::binary>>) do
    dbg("not complete #{inspect s.remaining}")

    {rest, s}
  end

  @doc """
    Read geometry features from a byte stream

    ```
      File.stream!("rivers.shp", [], 2048)
      |> Exshape.Shp.read
      |> Stream.run
    ```

  Options:
    * `raise_on_parse_error: bool` - whether to throw an exception if a shape or dbf file is not completely consumed without error (default false)
    * `raise_on_nan_points: bool` - whether to throw an exception if a point with NaN coordinates is encountered (default false)
    * `native: bool` - whether to use native code for nesting polygon holes (default true)
  """
  def read(byte_stream, opts \\ []) do
    native = Keyword.get(opts, :native, true)
    raise_on_nan_points = Keyword.get(opts, :raise_on_nan_points, false)
    raise_on_parse_error = Keyword.get(opts, :raise_on_parse_error, false)

    state = %State{
      nest_polygon: if(native, do: &native_nest_polygon/1, else: &beam_nest_polygon/1),
      raise_on_nan_points: raise_on_nan_points
    }
    Stream.transform(byte_stream, {<<>>, state}, fn bin, {buf, state} ->
      case do_read(state, buf <> bin) do
        {buf, %State{mode: :done}} = s ->
          if raise_on_parse_error && buf != "" do
            raise Errors.ShpParseError
          else
            {:halt, s}
          end
        {buf, %State{emit: emit} = s} ->
          {Enum.reverse(emit), {buf, %{s | emit: []}}}
      end
    end)
  end

end
