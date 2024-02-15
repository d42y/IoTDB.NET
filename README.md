
# IoTDB.NET Library

## Overview

IoTDB.NET is optimized for C# applications, offering a lightweight alternative to traditional databases. IoTDB.NET design allows for effective management of time-series data, catering specifically to the needs of IoT applications.

## Features

- Light weight and fast time-series data storage
- Flexible Entity management
- Time-series data retrieval with support for interval and linear interpolation
- Thread-safe data handling

## Before You Continue

- This is beta release. 
- IoTDB.NET is designed to store values as the double data type, catering primarily to the numeric outputs commonly produced by IoT sensors. This structure is optimized for handling time series data that is numerical in nature. If your application requires the storage of time series data in non-numeric formats, IoTDB.NET may not suit your needs.
- IoTDB.NET entities can have properties as string data type. However, those properties are not time series data.

## Installation

To use the IoTDB.NET library in your project, follow these steps:

1. .NET environment compatible with C# .NET 7
2. Install IoTDB.NET NuGet Package.

## Quick Start

### Initializing the Database
IoTDB.NET use flat files to store the data. Make sure your application has write permission to the database path.

```csharp
using IoTDB.NET;

// Specify database name and path
var dbName = "MyIoTDatabase";
var dbPath = "path/to/database/directory";
bool createPathIfNotExist = true;

// Create an instance of IoTData
var iotData = new IoTData(dbName, dbPath, createPathIfNotExist);
```

### Create an Entity

An Entity serves as the unique identifier for your data within the system, structured as a dataset consisting of key-value pairs. Before you can begin storing time series data, it's essential to first create an entity and obtain an entityId. This entityId is crucial as it links your data to its specific identity, ensuring accurate organization and retrieval within the database.

Entities in IoTDB.NET can be uniquely identified using one or more key-value pairs (KVPs), where both the key and the value are strings. This flexible approach allows for detailed identification of entities based on various attributes. For example, a simple entity can be identified using a single identifier, such as ("Name", "Sensor_1").

For more complex scenarios requiring nuanced differentiation, entities can be distinguished using multiple identifiers. Consider the case of two sensors:

The first sensor might be identified by a combination of attributes: ("Name", "Sensor_2"), ("Controller", "1"), ("Network", "2").
A second sensor, despite sharing a similar name, is uniquely identified by a different set of attributes: ("Name", "Sensor_2"), ("Controller", "2"), ("Network", "2").
This system allows for precise entity management, enabling you to specify and query entities based on a rich, multi-dimensional attribute space, enhancing the robustness and granularity of your IoT data management.

Create an Enity:
```csharp
// Create an Enity. 
await iotData.Entity.AddEntityAsync((PropertyName.Name, $"Sensor_1}"));

```

The PropertyName class introduces a collection of standard identifiers, aimed at reducing typographical errors during the creation and retrieval of Entities. These standardized identifiers are strongly encouraged to enhance consistency and diminish errors. However, the system is built with versatility in mind, permitting the use of arbitrary strings as property names. This design choice offers developers the freedom to expand the database schema according to their requirements without limitations, while still leveraging the ease and dependability provided by standard identifiers whenever suitable.

### Retrieving an Entity
To guarantee precise retrieval of entities, it's imperative to use the identifiers specifically established for them. Utilizing these identifiers correctly significantly reduces the possibility of mistakenly retrieving an incorrect entity, thereby safeguarding the integrity and reliability of your data retrieval procedures.

```csharp
// Retrieving the entity

var entity = await data.Entity.GetEntityAsync((PropertyName.Name, $"Sensor_1"));

// entity data type is (string Id, string Guid). 

```

### Adding and Retrieving Data

```csharp
// Set data for an entity
int entityId = entity.Id;
double value = 23.5; 
DateTime timestamp = DateTime.Now; // Use specific timestamp or default for current time
iotData.Set(entityId, value, timestamp, true); // true indicates time-series data

// Asynchronously retrieve data for an entity
var result = await iotData.GetAsync(entityId); //Get last value

// result is nullable data type (double Value, DateTime Timestamp).
// In IoTDB.NET, all timestamps are recorded in Universal Time (UTC) to ensure consistency and accuracy across different time zones and systems.

if (result.HasValue)
{
    Console.WriteLine($"Value: {result.Value.Value}, Timestamp: {result.Value.Timestamp}");
}
```

### Getting Time-Series Data

```csharp
// Define entity IDs and time range
List<int> entityIds = new List<int> { 1, 2, 3 }; 
DateTime from = DateTime.Now.AddDays(-1);
DateTime to = DateTime.Now;

// Asynchronously retrieve time-series data for multiple entities
var timeSeriesData = await iotData.GetAsync(entityIds, from, to);
foreach (var item in timeSeriesData)
{
    Console.WriteLine($"Entity ID: {item.Key}");
    foreach (var data in item.Value)
    {
        // The Timestamp property of a TimeSeriesItem is not directly represented as a C# DateTime object. 
        // To obtain a DateTime representation, you can utilize the .ToDateTime method for UTC time or .ToLocalDateTime for local time 
        Console.WriteLine($"Timestamp: {data.ToLocalDateTime}, Value: {data.Value}");
    }
}
```


### Getting Time-Series Data with Interval
The interval feature enables the retrieval of data at specified intervals. In cases where data points are missing, they will be filled in through linear interpolation to ensure continuity in the dataset.

```csharp
// Define entity IDs and time range
List<int> entityIds = new List<int> { 1, 2, 3 }; 
DateTime from = DateTime.Now.AddDays(-1);
DateTime to = DateTime.Now;
int interval = 1;
IntervalType itype = IntervalType.Seconds;

// Asynchronously retrieve time-series data for multiple entities
var timeSeriesData = await iotData.GetAsync(entityIds, from, to, interval, itype);
foreach (var item in timeSeriesData)
{
    Console.WriteLine($"Entity ID: {item.Key}");
    foreach (var data in item.Value)
    {
        Console.WriteLine($"Timestamp: {data.ToLocalDateTime}, Value: {data.Value}");
    }
}
```

### Exception Handling in IoTDB.NET
IoTDB.NET enforces strict exception handling during its boot-up process. It is imperative to catch and manage these exceptions to ensure smooth operation. For exceptions that occur at runtime, IoTDB.NET provides a mechanism to subscribe to an event notification system that alerts you to these exceptions.

Unhandled exceptions can significantly disrupt high-volume processing systems, potentially slowing them down or, in the worst-case scenario, leading to data loss. It is crucial to diligently manage all exceptions to prevent any unintended consequences. Ensuring comprehensive exception handling will help maintain the integrity and reliability of your system.

Here's how you can subscribe to and handle exceptions:

```csharp
// Subscribe to the exception event
iotData.ExceptionOccurred += OnExceptionOccurred;

// Event handler for exceptions
private static void OnExceptionOccurred(object? sender, ExceptionEventArgs e)
{
    // Log or handle the exception as needed
    Console.WriteLine($"Exception occurred: {e.Message}");
    // The ExceptionEventArgs includes details about the exception:
    // - Class: Name of the class where the exception occurred
    // - Method: Name of the method where the exception occurred
    // - Type: Full name of the exception type
    // - Message: Message of the exception
    // - Timestamp: Time when the exception occurred, in UTC. Convert to local time with .ToLocalDateTime.
}

```



## Contributing

Beta relase. No outside contribution at this time.

## License
This library is licensed under the MIT License. This means you are free to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the software, provided you include the following conditions in your distribution:

1. A copy of the original MIT License and copyright notice must be included with the software.
2. The software is provided "as is", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and non-infringement. In no event shall the authors or copyright holders be liable for any claim, damages, or other liability, whether in an action of contract, tort or otherwise, arising from, out of, or in connection with the software or the use or other dealings in the software.


This permissive license encourages open and collaborative software development while providing protection for the original authors. For more details, please refer to the full MIT License text.

## Third-Party Licenses and Acknowledgments

This software includes and/or depends on the following third-party software component, which is subject to its own license:

- **TeaFile**: TeaFile is used for efficient time series data storage and access. License: For specific license terms, please refer to the [TeaFile web page](http://discretelogics.com/teafiles/#license).

We express our gratitude to the contributors and maintainers of TeaFile for their work.

