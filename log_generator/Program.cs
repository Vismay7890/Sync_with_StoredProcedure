using latlog;

namespace lognew
{
    class Program
    {
        static void Main()
        {

            Latlog.LogMessage();
            Latlog.Log(LogLevel.Info, "This is an information");
            System.Console.WriteLine("Log file created successfully. Press any key to exit.");
        }
    }
}
