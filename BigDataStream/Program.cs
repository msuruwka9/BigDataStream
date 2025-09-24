using System.Runtime.CompilerServices;
using Newtonsoft.Json.Linq;

var cancellationTokenSource = new CancellationTokenSource();
var token = cancellationTokenSource.Token;
int numberOfRecords = 0;
int loggedIn = 0;
int loggedOut = 0;
float sumOfSongLenght = 0;
int songLengthRecords = 0;


await foreach (var obj in FilterByLoggedAsync("D:\\data.json", true, 3_000_000, token))
{
    if (obj["auth"]?.ToString() == "Logged In")
    {
        loggedIn++;
    }
    
    if (obj["auth"]?.ToString() == "Logged Out")
    {
        loggedOut++;
    }

    if (obj["length"] is not null)
    {
        sumOfSongLenght += obj["length"].Value<float>();
        songLengthRecords++;
    }
    
    if (++numberOfRecords % 1_000_000 == 0 && numberOfRecords != 1)
    {
        Console.WriteLine($"Total number of records: {numberOfRecords}");
        Console.WriteLine($"Logged in users: {loggedIn}");
        Console.WriteLine($"Logged out users: {loggedOut}");
        Console.WriteLine($"Average song length: {sumOfSongLenght / songLengthRecords}");
    }
}

return;

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