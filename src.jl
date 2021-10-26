using CordraClient
using NeXLParticle
using PeriodicTable
using DataFrames
using Dates
using TimeZones
using DataStructures
using UUIDs

path = "/media/camilovelezr/D/NIST/GSRData_Management/Shooter #1 - Zero time/APA/Analysis 2019-07-17 10.58.57.-0400/data.hdz"


eldict = Dict(zip(map(uppercase, [x.symbol for x in elements]), [x.symbol for x in elements])) # HE=>He dicitonary to normalize elements' symbols

"""
Function to normalize column names of periodic table elements in UPPERCASElowercase format. (He, Mg, Na...)
"""
function renel(x::AbstractString)::AbstractString
    !(haskey(eldict, x)) && return x
    return eldict[x]
end

t = Zeppelin(path)

"""
    normalize_elements(z::Zeppelin)
Apply `renel` to a `Zeppelin` object to rename `Zeppelin.data` columns.
"""
function normalize_elements(z::Zeppelin)
    rename!(renel, z.data)
end

last(names(t.data), 17)
# mut struct?
normalize_elements(t)

last(names(t.data), 17)

"""
    cleandate(x::AbstractString)::Date
    cleandate(x::AbstractString, f::DateFormat)::Date

Converts string to `Date`. If no `DateFormat` provided it will assume "mm/dd/yyyy".
"""
function cleandate(x::AbstractString)::Date
    return Dates.Date(x, "mm/dd/yyyy")
end

function cleandate(x::AbstractString, f::DateFormat)::Date
    return Dates.Date(replace(x, "/"=>"-"), f)
end

t.header["ANALYSIS_DATE"]

"""
    splitvalue(x::AbstractString)::AbstractDict

Splits the value of an entry into a dictionary of {value: ___, unitText: ___}.
For example 0.92334 nA becomes {value: 0.92334, unitText: nA}.

If no unitText in original string, it defaults to mm.
"""
function splitvalue(x::AbstractString)::AbstractDict
    s = split(x)
    return Dict(["value"=>tryparse(Float64, s[1]),
                "unitText"=> length(s)==2 ? s[2] : "mm"])
end

h = t.header

"""
    tzone(x::AbstractString)::AbstractString

Replaces EDT, EST, MDT for its respective UTC offset. For example "EDT" is replaced with "-0400"
"""
function tzone(x::AbstractString)::AbstractString
    s = split(x)
    s[end] == "EDT" && return replace(x, "EDT"=>"-0400")
    s[end] == "EST" && return replace(x, "EST"=>"-0500")
    s[end] == "MDT" && return replace(x, "MDT"=>"-0600")
end

"""
Creates a `DateTime` object from a date and a time and convert its time to UTC.
"""
function to_datetime(date::Date, time::AbstractString)::DateTime
    return ZonedDateTime(string(date)*" "*tzone(time), "yyyy-mm-dd I:M:S p zzzz").utc_datetime
end

function to_datetime(date::AbstractString, time::AbstractString)::DateTime
    return ZonedDateTime(date*" "*tzone(time), "mm/dd/yyyy I:M:S p zzzz").utc_datetime
end

function to_datetime(date::AbstractString, time::AbstractString, format::AbstractString)::DateTime
    return ZonedDateTime(date*" "*tzone(time), format).utc_datetime
end


function magkv(x::AbstractString, unit::AbstractString)::AbstractDict # Vector or Dict?
    return Dict(["value" => tryparse(Float64, x), "unitText"=>unit])
end

"""
    mapmag(z::Zeppelin)
Returns `Zeppelin.headers` as a `Dict{String, Any}` including the mapping of {MAG_FMT, MAG0} key-value pairs.
"""
function mapmag(z::Zeppelin)
    h = convert(Dict{String, Any}, Dict(copy(z.header)))
    mag_fmt = collect(map(uppercase, split(h["MAG_FMT"])))
    mag0 = split(h["MAG0"])
    h[mag_fmt[1]*"_DATA"] = magkv(mag0[1], "Assuming a 3.5 in field of view")
    h[mag_fmt[2]] = magkv(mag0[2], "")
    h[mag_fmt[3]] = magkv(mag0[3], "")
    h[mag_fmt[4]*"_DATA"] = magkv(mag0[4], "sq mm")
    return h
end

function clean_headers(z::Zeppelin)
    d = convert(OrderedDict{String, Any}, Dict(copy(z.header)))

    d["ANALYSIS_DATE"] = cleandate(d["ANALYSIS_DATE"])
    d["DATETIME"] = to_datetime(d["ANALYSIS_DATE"], d["START_TIME"])

    d["ACCELERATING_VOLTAGE"] = splitvalue(d["ACCELERATING_VOLTAGE"])
    d["PROBE_CURRENT"] = splitvalue(d["PROBE_CURRENT"])

    mag_fmt = collect(map(uppercase, split(d["MAG_FMT"])))
    mag0 = split(d["MAG0"])
    d[mag_fmt[1]*"_DATA"] = magkv(mag0[1], "Assuming a 3.5 in field of view")
    d[mag_fmt[2]] = magkv(mag0[2], "")
    d[mag_fmt[3]] = magkv(mag0[3], "")
    d[mag_fmt[4]*"_DATA"] = magkv(mag0[4], "sq mm")

    return d
end



mutable struct ZepUp # Zeppelin to upload to Cordra
    z::Zeppelin
    samplemeta::AbstractDict
    morph::DataFrame

    
    function ZepUp(
        headerfile::AbstractString
    )
        morph_cols = collect(NeXLParticle.MORPH_COLS)
        z = Zeppelin(headerfile)
        normalize_elements(z)
        new(
            z,
            clean_headers(z),
            select(z.data, morph_cols)
        )
    end
 end


test = ZepUp(path)



function upload_metadata(z::ZepUp, cc::CordraConnection)
    obj = z.samplemeta
    obj["CamiloExplore"] = 1
    uuid = split(string(UUIDs.uuid4()), "-")[2]
    create_object(cc, obj, "Material", suffix = "gsr-2019-"*uuid)
end

cs = CordraConnection("https://localhost:8443", "admin"; verify = false)

upload_metadata(test, cs)


