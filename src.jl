using CordraClient
using NeXLParticle
using PeriodicTable
using DataFrames
using Dates
using TimeZones
using DataStructures

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

names(t.data)


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


function magkv(x::AbstractString, unit::AbstractString)::AbstractVector
    return ["value" => tryparse(Float64, x), "unitText"=>unit]
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



