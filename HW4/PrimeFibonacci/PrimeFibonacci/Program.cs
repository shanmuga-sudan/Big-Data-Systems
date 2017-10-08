using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PrimeFibonacci
{
    class Program
    {
       
        static void Main(string[] args)
        {
            Console.WriteLine("Hit Enter to print the primonnacci transformation..");
            Console.ReadLine();
            var primonacci = Unfold5(3, 5, (a, b) => a + b);
            foreach (var x in primonacci.Skip(10000).Take(1))
            {
                Console.WriteLine(x);

            }
            Console.ReadLine();

        }
        private static IEnumerable<T> Unfold<T>(T seed, Func<T, T> accumulator)
        {
            var counter = seed;
            while (true)
            {
                yield return counter;
                counter = accumulator(counter);
            }
        }

        private static ArrayList Unfold3(int PrimeNumber, int ChooseNumber)
        {
            ArrayList primeNos = new ArrayList();
            primeNos.Add(2);

            while (primeNos.Count < ChooseNumber)
            {
                int sqrt = (int)Math.Sqrt(PrimeNumber);
                bool isPrime = true;
                for (int i = 0; (int)primeNos[i] <= sqrt; i++)
                {
                    if (PrimeNumber % (int)primeNos[i] == 0)
                    {
                        isPrime = false;
                        break;
                    }
                }
                if (isPrime)
                {
                    primeNos.Add(PrimeNumber);
                }
                PrimeNumber += 2;
            }

            return primeNos;

        }

        private static IEnumerable<T> Unfold5<T>(T seed1, T seed2, Func<T, T, T> accumulator)
        {
            var a = seed1;
            var b = seed2;
            yield return a;
            yield return b;
            T c;
            while (true)
            {

                c = b;
                b = accumulator(a, b);
                a = c;
                int c1 = Convert.ToInt32(b);
                int num1 = c1;
                bool isPrime = true;

                for (int k = 3; k < num1; k++)
                {
                    if (num1 % k == 0)
                    {
                        isPrime = false;
                        break;
                    }
                }

                if (isPrime)
                {
                    yield return b;
                }

            }

            //return 0;
        }

        // [][] fibonacci numbers transofmration (coroutine)
        private static IEnumerable<T> Unfold2<T>(T seed, Func<T, T, T> accumulator)
        {
            var a = seed;
            var b = seed;
            T c;
            while (true)
            {
                yield return b;
                c = b;
                b = accumulator(a, b);
                a = c;
            }

            //return 0;
        }

    }
    }


/*      Console.WriteLine("The Following is 10,001 prime number generated using LINQ and lambda expressions in C#  :");
      Console.ReadLine();
         Console.WriteLine("Hit RETURN to find the 10,001 primonacci transformation..");
         Console.ReadLine();
         var fibonacci = Unfold2(1, (a, b) => a + b);
         IEnumerable<int> primeNo = fibonacci.TakeWhile(number=>Enumerable.Range(2, (int)Math.Sqrt(number)).All(divisor => number % divisor != 0));
         primeNo
               .Skip(2).Take(3)
               .ToList()
               .ForEach(prime => Console.WriteLine(prime));
         Console.ReadLine();
                 primeNo
                       .Skip(2).Take(1)                                   // Consider 1st number after skipping 10,000 to be generated.
                      .ToList()
                      .ForEach(prime => Console.WriteLine(primeNo)); // Print the prime number generated
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

     private static IEnumerable<T> Unfold2<T>(T seed, Func<T, T, T> accumulator)
     {
         var a = seed;
         var b = seed;
         T c;
         while (true)
         {
             yield return b;
             c = b;
             b = accumulator(a, b);
             a = c;
         }
     }
      */
