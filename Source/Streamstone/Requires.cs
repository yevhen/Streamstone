using System;

namespace Streamstone
{
    using Annotations;
    
    static class Requires
    {
        [AssertionMethod]
        public static void NotNull<T>(T argument, [InvokerParameterName] string argumentName) where T : class 
        {
            if (argument == null)
                throw new ArgumentNullException(argumentName);
        }

        [AssertionMethod]
        public static void NotNullOrEmpty(string argument, [InvokerParameterName] string argumentName)
        {
            if (argument == null)
                throw new ArgumentNullException(argumentName);

            if (argument == "")
                throw new ArgumentException(argumentName + " cannot be an empty string", argumentName);
        }

        [AssertionMethod]
        public static void GreaterThanOrEqualToZero(long argument, [InvokerParameterName] string argumentName)
        {
            if (argument < 0)
                throw new ArgumentOutOfRangeException(argumentName, argumentName + " should be >= 0");
        }

        [AssertionMethod]
        public static void GreaterThanOrEqualToOne(long argument, [InvokerParameterName] string argumentName)
        {
            if (argument < 1)
                throw new ArgumentOutOfRangeException(argumentName, argumentName + " should be >= 1");
        }
    }
}