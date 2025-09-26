using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Newtonsoft.Json.Linq;

string FilePath = "D:\\data.json";
const int BatchSize = 10_000;
const int ConsumerCount = 20; 

Console.WriteLine("Uruchamiam przetwarzanie równoległe z użyciem wzorca Producent-Konsument...");
var stopwatch = Stopwatch.StartNew();

var channel = Channel.CreateBounded<List<JObject>>(ConsumerCount);
var finalResults = new FinalResultsAggregator();
        
var cts = new CancellationTokenSource();

Task producerTask = ProduceAsync(channel.Writer, FilePath, BatchSize, cts.Token);

List<Task> consumerTasks = new();
for (int i = 1; i <= ConsumerCount; i++)
{
    consumerTasks.Add(ConsumeAsync(channel.Reader, finalResults, i, cts.Token));
}

await producerTask;
Console.WriteLine("Producent zakończył pracę. Oczekuję na przetworzenie ostatnich paczek przez konsumentów...");

await Task.WhenAll(consumerTasks);

stopwatch.Stop();
Console.WriteLine($"\nPrzetwarzanie zakończone w {stopwatch.Elapsed.TotalSeconds:F2}s.");

finalResults.PrintResults();

return;

async Task ConsumeAsync(ChannelReader<List<JObject>> reader, FinalResultsAggregator aggregator, int consumerId, CancellationToken ct)
{
    Console.WriteLine($"Konsument #{consumerId}: Oczekuję na dane...");

    try
    {
        await foreach (var batch in reader.ReadAllAsync(ct))
        {
            long localLoggedIn = 0;
            long localLoggedOut = 0;
            double localTotalLength = 0;
            long localSongRecords = 0;

            foreach (var record in batch)
            {
                var authStatus = record["auth"]?.ToString();
                if (authStatus == "Logged In") localLoggedIn++;
                else if (authStatus == "Logged Out") localLoggedOut++;

                if (record.TryGetValue("length", out var lengthToken) && lengthToken.Type == JTokenType.Float)
                {
                    localTotalLength += lengthToken.Value<double>();
                    localSongRecords++;
                }
            }
            
            aggregator.AddBatchResults(batch.Count, localLoggedIn, localLoggedOut, localTotalLength, localSongRecords);
            Console.WriteLine($"Konsument #{consumerId}: Przetworzono paczkę {batch.Count} rekordów.");
        }
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine($"Konsument #{consumerId}: Operacja anulowana.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Błąd krytyczny w konsumencie #{consumerId}: {ex.Message}");
    }
    
    Console.WriteLine($"Konsument #{consumerId}: Zakończono pracę.");
}

async Task ProduceAsync(ChannelWriter<List<JObject>> writer, string filePath, int batchSize, CancellationToken ct)
{
    Console.WriteLine("Producent: Rozpoczynam czytanie pliku...");
    var batch = new List<JObject>(batchSize);

    try
    {
        await foreach (var record in ReadRecordAsync(filePath, ct))
        {
            batch.Add(record);
            if (batch.Count == batchSize)
            {
                await writer.WriteAsync(batch, ct);
                batch = new List<JObject>(batchSize);
            }
        }

        if (batch.Count > 0)
        {
            await writer.WriteAsync(batch, ct);
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Błąd krytyczny w producencie: {ex.Message}");
    }
    finally
    {
        writer.Complete();
        Console.WriteLine("Producent: Zakończono czytanie i zamknięto kanał.");
    }
}

async IAsyncEnumerable<JObject> FilterByLoggedAsync(string filePath, bool loggedInOnly, int stopNumber, [EnumeratorCancellation] CancellationToken ct)
{
    var foundCount = 0;
    var targetStatus = loggedInOnly ? "Logged In" : "Logged Out";
    await foreach (var obj in ReadRecordAsync(filePath, ct))
    {
        if (obj["auth"]?.Value<string>() == targetStatus)
        {
            foundCount++;
            yield return obj;
        }

        if (foundCount == stopNumber)
            break;
    }
}

async IAsyncEnumerable<JObject> ReadRecordAsync(string filePath,[EnumeratorCancellation] CancellationToken cancellationToken)
{
    var fileStream = File.Open(filePath, FileMode.Open);
    using var streamReader = new StreamReader(fileStream);

    string? line;
    
    while (!streamReader.EndOfStream)
    {
        line = await streamReader.ReadLineAsync(cancellationToken);
        
        if (string.IsNullOrWhiteSpace(line))
            yield return new JObject();


        yield return JObject.Parse(line);
    }
}

public class FinalResultsAggregator
{
    private long _totalRecords = 0;
    private long _loggedInCount = 0;
    private long _loggedOutCount = 0;
    private double _totalSongLength = 0;
    private long _songLengthRecordsCount = 0;
    
    private readonly object _lengthLock = new();

    public void AddBatchResults(long total, long loggedIn, long loggedOut, double totalLength, long songRecords)
    {
        Interlocked.Add(ref _totalRecords, total);
        Interlocked.Add(ref _loggedInCount, loggedIn);
        Interlocked.Add(ref _loggedOutCount, loggedOut);
        Interlocked.Add(ref _songLengthRecordsCount, songRecords);

        lock (_lengthLock)
        {
            _totalSongLength += totalLength;
        }
    }

    public void PrintResults()
    {
        Console.WriteLine("\n--- Końcowe podsumowanie ---");
        Console.WriteLine($"Łącznie rekordów: {_totalRecords:N0}");
        Console.WriteLine($"Zalogowani: {_loggedInCount:N0}");
        Console.WriteLine($"Wylogowani: {_loggedOutCount:N0}");

        double averageLength = _songLengthRecordsCount > 0 ? _totalSongLength / _songLengthRecordsCount : 0;
        Console.WriteLine($"Liczba utworów z długością: {_songLengthRecordsCount:N0}");
        Console.WriteLine($"Średnia długość utworu: {averageLength:F2}s");
    }
}