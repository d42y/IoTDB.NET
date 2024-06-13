using IoTDBdotNET;

namespace ConsoleApp
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // Specify database name and path
            var dbName = "MyIoTDatabase";
            var dbPath = @"c:\temp";

            // Create an instance of IoTData
            var iotData = new IoTDatabase(dbName, dbPath, "encryption password");

            // Create a table with class name as table name
            var friendTbl = iotData.Tables<Friend>();

            //check if table has friend name Bob
            var friend = friendTbl.FindOne(x => x.Name.Equals("bob", StringComparison.OrdinalIgnoreCase));
            if (friend == null)
            {
                //create new friend
                friend = new Friend() { Name = "Bob" };
                //insert friend to database
                var id = friendTbl.Insert(friend);
                if (id.IsNull)
                {
                    Console.WriteLine("Failed to insert.");
                    return;
                }
            }

            //display record
            Console.WriteLine($"Success: Id [{friend.Id}] Name [{friend.Name}]");

            // Address table
            var addressTbl = iotData.Tables<Address>();

            //check if table has address with Bob's record id
            var address = addressTbl.FindOne(x => x.FriendId == friend.Id);

            if (address == null)
            {
                //create new friend
                address = new Address()
                {
                    FriendId = friend.Id,
                    Street = "123 Main St.",
                    City = "Friend Town",
                    State = "TX",
                    ZipCode = "75001-0001"
                };
                try
                {
                    //insert friend to database
                    var id = addressTbl.Insert(address);
                    if (id.IsNull)
                    {
                        Console.WriteLine("Failed to insert.");
                        return;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error insert address: [{ex.Message}]");
                    return;
                }
            }

            //display record
            Console.WriteLine($"Success: Id [{address.Id}] FriendId [{friend.Id}] Street [{address.Street}]");

        }
    }
}
