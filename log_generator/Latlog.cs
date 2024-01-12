using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace latlog
{

        public enum LogLevel
        {
            Info,
            Error,
            Debug
        }
        public class Latlog
        {
            private static readonly string logFolderPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Log");
            private static readonly string logFilePath;
            private static readonly object lockObject = new object();
            private static readonly int maxLogFileAgeDays = 5;

            static Latlog()
            {
                EnsureLogFolderExists();
                logFilePath = Path.Combine(logFolderPath, $"DLL_{DateTime.Now:yyyyMMdd_HHmmss}.log");
                DeleteOldLogFiles();
            }

            public static void LogMessage()
            {
                Log(LogLevel.Info, "This is an informational message.");
                Log(LogLevel.Error, "This is an error message.");
                Latlog.LogError("SomeFunction", "An error occurred", new InvalidOperationException("Sample exception message"));
                Log(LogLevel.Debug, "This is a Debug statement");
            }

            public static void Log(LogLevel level, string message)
            {
                try
                {
                   // EnsureLogFolderExists();

                    lock (lockObject)
                    {
                        using (StreamWriter writer = File.AppendText(logFilePath))
                        {
                            Console.WriteLine(message);
                            writer.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} [{level}] - {message}");
                        }
                    }

                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error logging: {ex.Message}");
                }
            }

            public static void LogError(string functionName, string errorMessage, Exception exception)
            {
                Log(LogLevel.Error, $"Error in {functionName}: {errorMessage}");

                // Log exception details
                if (exception != null)
                {
                    Log(LogLevel.Error, $"Exception details: {exception}");
                }
            }

            private static void EnsureLogFolderExists()
            {
                if (!Directory.Exists(logFolderPath))
                {
                    Directory.CreateDirectory(logFolderPath);
                }
            }

            private static void DeleteOldLogFiles()
            {
                try
                {
                    string[] logFiles = Directory.GetFiles(logFolderPath, "*.log");
                    foreach (var file in logFiles)
                    {
                        DateTime createDate = File.GetCreationTime(file);
                        if (DateTime.Now - createDate > TimeSpan.FromDays(maxLogFileAgeDays))
                        {
                            File.Delete(file);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error deleting old log files: {ex.Message}");
                }
            }
        }
    }
