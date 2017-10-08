using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PrimeNumbers
{
    class Prime
    {
       
        static void Main(string[] args)
        {
            Console.WriteLine("The Following results are prime numbers generated using LINQ and lambda expressions in C#  :");
            Console.ReadLine();
            IEnumerable<int> primeNumbers =
            Enumerable.Range(2, Int32.MaxValue - 1).Where(number => Enumerable.Range(2, (int)Math.Sqrt(number) - 1)  //Considered integer range for the process
           .All(divisor => number % divisor != 0));         //check if the taken number is prime.
            primeNumbers
                .Take(10)                                   // Consider 10 numbers to be generated.
                .ToList()
                .ForEach(prime => Console.WriteLine(prime)); // Print the prime number generated
            Console.ReadLine();
        }
    }
}
