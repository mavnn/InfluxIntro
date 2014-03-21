#r "c:\DevEdStuff\InfluxIntro\Fsharpx.Core.dll"
#r "c:\DevEdStuff\InfluxIntro\Newtonsoft.Json.dll"
#r "System.Net.Http"
#r "System.Web"
open System


[<AutoOpen>]
module Configuration =

    type InfluxDBConfiguration =
        { Client: InfluxDBClientConfiguration
          Server: InfluxDBServerConfiguration }

    and InfluxDBServerConfiguration =
        { Host: string
          Port: int
          Database: string
          Username: string
          Password: string }

    and InfluxDBClientConfiguration =
        { MaxBufferSize: int
          MaxBufferTime: int }


[<AutoOpen>]
module Series =

    type InfluxSeries () =
        member val Name = String.Empty with get, set
        member val Columns = Array.empty<string> with get, set
        member val Points = Array.empty<obj array> with get, set


[<AutoOpen>]
module internal Buffering =

    open FSharpx

    type Buffer = Map<(string * string), obj list>
    
    let append buffer series e =
        buffer |> Lens.update 
            (fun x -> match x with | Some es -> Some (es @ [e]) | _ -> Some [e])
            (Lens.forMap (series, e.GetType().FullName))

    let makeBuffer () =
        Map.empty<(string * string), obj list>

    let size (buffer: Buffer) =
        buffer |> Map.fold (fun count _ events -> count + events.Length) 0


[<AutoOpen>]
module internal Serialization =

    open Newtonsoft.Json
    open Newtonsoft.Json.Serialization

    let private settings =
        JsonSerializerSettings (
            ContractResolver = CamelCasePropertyNamesContractResolver (),
            Formatting = Formatting.Indented)

    let map (buffer: Buffer) =
        buffer 
        |> Map.fold (fun series key value ->
            let columns, points =
                value.[0].GetType().GetProperties ()
                |> (fun properties ->
                    properties 
                    |> Array.map (fun p -> p.Name),
                    value 
                    |> Seq.map (fun e -> properties |> Array.map (fun p -> box (p.GetValue e))) 
                    |> Seq.toArray)
                     
            series @ [InfluxSeries (Name = fst key, Columns = columns, Points = points)]) List.empty
        |> List.toArray

    let serialize (series: InfluxSeries array) =
        JsonConvert.SerializeObject (series, settings)

    let deserialize<'T> json =
        JsonConvert.DeserializeObject<'T> json

    let deserializeSeries (json) =
        JsonConvert.DeserializeObject<InfluxSeries array>(json)


[<AutoOpen>]
module internal Transmission =   

    open System.Net
    open System.Net.Http
    open System.Net.Http.Headers
    open System.Text
    open System.Web

    let buildUri (config: InfluxDBServerConfiguration) queries =
        let query = HttpUtility.ParseQueryString ("")
        query.["u"] <- config.Username
        query.["p"] <- config.Password
        queries |> List.iter (fun (name, value) -> query.[name] <- value)

        let uri = UriBuilder ()
        uri.Host <- config.Host
        uri.Port <- config.Port
        uri.Path <- sprintf "db/%s/series" config.Database
        uri.Query <- query.ToString ()
        uri

    let flush config buffer : Buffer =
        let uri = buildUri config []

        use content = new StringContent ((map >> serialize) buffer, Encoding.UTF8, "application/json")
        use http = new HttpClient ()

        let result =
            http.PostAsync (uri.Uri, content)
            |> Async.AwaitTask
            |> Async.RunSynchronously

        makeBuffer ()

    let get config query =
        async {
            let uri = buildUri config ["q", query]

            use http = new HttpClient ()
            let! response =
                http.GetAsync(uri.Uri, HttpCompletionOption.ResponseHeadersRead)
                |> Async.AwaitTask
            if response.IsSuccessStatusCode then
                return Choice1Of2 (response.Content.ReadAsStreamAsync() |> Async.AwaitTask)
            else
                return Choice2Of2 (response.StatusCode |> int, response.ReasonPhrase, response.Content.ReadAsStreamAsync() |> Async.AwaitTask)
        }

[<AutoOpen>]
module Client =

    open FSharp.Control
    open FSharpx.Choice
    open Transmission
    open Newtonsoft.Json.Linq

    type private InfluxDBClientProtocol =
        | Send of string * obj

    type QueryFailure =
        { HttpCode : int; ErrorPhrase : string; Content : string }

    type QueryResult<'T> =
        { SeriesName : string; Values : seq<'T> }

    type InfluxDBClient (config: InfluxDBConfiguration) =
        let flush = flush config.Server

        let writeAgent = Agent.Start (fun agent ->
            let rec loop buffer =
                async {
                    let! msg = agent.TryReceive (config.Client.MaxBufferTime)

                    let buffer, doFlush =
                        match msg with
                        | (Some (Send (series, event))) ->
                            append buffer series event, size buffer >= (config.Client.MaxBufferSize - 1)
                        | _ ->
                            buffer, size buffer > 0

                    let buffer =
                        match doFlush with
                        | true -> flush buffer
                        | _ -> buffer

                    return! loop buffer }
                        
            loop (makeBuffer ()))

        member x.Write (series, data) =
            writeAgent.Post (Send (series, data))

        member x.QueryRaw query =
            get config.Server query

        member x.Query<'T> query =
            async {
                let success (aStream : Async<IO.Stream>) =
                    async {
                        let! stream = aStream
                        use sr = new IO.StreamReader(stream : IO.Stream)
                        let! content = sr.ReadToEndAsync() |> Async.AwaitTask
                        let series = deserializeSeries content
                        return
                            series
                            |> Seq.map (fun series -> 
                                let j = JObject()
                                series.Points
                                |> Seq.map (fun values ->
                                    let j = JObject()
                                    Seq.zip series.Columns values
                                    |> Seq.iter (fun (name, value) -> j.Add(name, JValue value))
                                    series.Name, deserialize<'T> <| j.ToString()))
                            |> Seq.concat
                            |> Seq.groupBy (fun (seriesName, value) -> seriesName)
                            |> Seq.map (fun (seriesName, valueTuples) -> 
                                { 
                                    SeriesName = seriesName
                                    Values = valueTuples |> Seq.map (fun (_, value) -> value) 
                                })
                            |> Choice1Of2
                    }
                let fail (error : int * string * Async<IO.Stream>) =
                    async {
                        let code, phrase, aStream = error
                        let! stream = aStream
                        use sr = new IO.StreamReader(stream : IO.Stream)
                        let! content = sr.ReadToEndAsync() |> Async.AwaitTask
                        return Choice2Of2 { HttpCode = code; ErrorPhrase = phrase; Content = content }
                    }                    
                let! result = get config.Server query
                return!
                    result
                    |> choice success fail
            }
