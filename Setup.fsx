#I "."
#load "FSharp.Charting\FSharp.Charting.fsx"
#load "FsEye\FsEye.fsx"
#load "Influx.fsx"

open FSharp.Charting
open Influx.Configuration
open Influx.Series
open Influx.Client

let config = {
    Client = { MaxBufferSize = 50; MaxBufferTime = 100 }
    Server = {
                Host = "influx.local"
                Port = 8086
                Database = "mydb"
                Username = "myUsername"
                Password = "myPassword"
        }
    }

let client = InfluxDBClient(config)
