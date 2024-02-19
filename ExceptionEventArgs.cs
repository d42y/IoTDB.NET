using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace IoTDBdotNET
{
    public class ExceptionEventArgs
    {
        public string Class { get; }
        public string Method { get; }
        public string Type { get; }
        public string Message { get; }
        public DateTime Timestamp { get; }
        public ExceptionEventArgs(Exception exception)
        {
            var names = GetName(exception);
            if (names != null && names.HasValue)
            {
                Class = names.Value.ClassName;
                Method = names.Value.MethodName;
            }
            else
            {
                Class = "Unknown";
                Method = "Unknown";
            }
            Type = exception.GetType().FullName ?? "";
            Message = exception.Message;
            Timestamp = DateTime.UtcNow;
        }

        public DateTime ToLocalDateTime => Timestamp.ToLocalTime();
        private (string ClassName, string MethodName)? GetName(Exception ex)
        {
            string stackTrace = ex?.StackTrace ?? "";
            // Assuming the stack trace is not empty and in a standard format
            if (!string.IsNullOrEmpty(stackTrace))
            {
                // Split the stack trace into lines
                string[] lines = stackTrace.Split(new[] { Environment.NewLine }, StringSplitOptions.None);
                // Get the first line of the stack trace, which typically contains the immediate method call that threw the exception
                string firstLine = lines.FirstOrDefault() ?? "";

                if (!string.IsNullOrEmpty(firstLine))
                {
                    // Split the line into parts to extract the method name and potentially the class name
                    // This is a simplistic approach and may need to be adapted based on actual stack trace formats
                    string[] parts = firstLine.Split(new[] { " at " }, StringSplitOptions.None);
                    if (parts.Length > 1)
                    {
                        string methodPart = parts[1];
                        // Example format: Namespace.ClassName.MethodName
                        // Further parsing may be required to cleanly separate class name from method name
                        string[] methodParts = methodPart.Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
                        if (methodParts.Length >= 2)
                        {
                            string className = methodParts[methodParts.Length - 2]; // Second to last part for class name
                            string methodName = methodParts.Last(); // Last part for method name, might include parameters and need further cleaning

                            // Optionally clean up methodName to remove parameters and other details
                            int paramsIndex = methodName.IndexOf('(');
                            if (paramsIndex != -1)
                            {
                                methodName = methodName.Substring(0, paramsIndex);
                            }

                            return (className, methodName);
                        }
                    }
                }

            }
            return null;
        }
    }
}
