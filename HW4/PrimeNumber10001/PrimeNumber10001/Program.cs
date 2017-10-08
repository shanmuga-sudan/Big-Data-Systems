using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PrimeNumber10001
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("The Following is 10,001 prime number generated using LINQ and lambda expressions in C#  :");
            Console.ReadLine();
            IEnumerable<int> primeNumbers =
            Enumerable.Range(2, Int32.MaxValue - 1).Where(number => Enumerable.Range(2, (int)Math.Sqrt(number) - 1)  //Considered integer range for the process
           .All(divisor => number % divisor != 0));         //check if the taken number is prime.
            primeNumbers
                .Skip(10000).Take(1)                                   // Consider 1st number after skipping 10,000 to be generated.
                .ToList()
                .ForEach(prime => Console.WriteLine(prime)); // Print the prime number generated
            Console.ReadLine();
        }
    }
}
