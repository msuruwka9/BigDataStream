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
    var found = 0;
    await foreach (var obj in ReadRecordAsync(filePath, ct))
    {
        if (loggedInOnly)
        {
            yield return CheckLoginStatus("Logged In", obj, ref found);
        }
        else
        {
            yield return CheckLoginStatus("Logged Out", obj, ref found);
        }

        if (found == stopNumber)
            break;
    }
}

static JObject CheckLoginStatus(string status, JObject obj, ref int counter)
{
    if (obj["auth"]?.ToString() != status) 
        return new JObject();
    
    counter++;
    return obj;
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