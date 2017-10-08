using System;
// using System.Collections.Generic;
// using System.Linq;
// using System.Threading.Tasks;
using System.IO;

namespace genetic_algorithm
{
    public class Program
    {
        public static void Main(string[] args)
        {

           int popSize = 8;
            int maxGeneration = 500;
            double exitError = 0.0;
            double mutateRate = 0.20;
            double mutateChange = 0.01;
            double tau = 0.40;

            Console.WriteLine("\nSetting popSize = " + popSize);
            Console.WriteLine("Setting maxGeneration = " + maxGeneration);
            Console.Write("Setting early exit MSE error = ");
            Console.WriteLine(exitError.ToString("F3"));
            Console.Write("Setting mutateRate = ");
            Console.WriteLine(mutateRate.ToString("F3"));
            Console.Write("Setting mutateChange = ");
            Console.WriteLine(mutateChange.ToString("F3"));
            Console.Write("Setting tau = ");
            Console.WriteLine(tau.ToString("F3"));

                Console.WriteLine("Creating a 4-6-3 neural network");
            Console.WriteLine("Using tanh and softmax activations");
            const int numInput = 4;
            const int numHidden = 5;
            const int numOutput = 3;
            ann nn =
            new ann(numInput, numHidden, numOutput);

           

              double[][] allData = LoadData("IrisData3.txt", 150, 7); // 150 rows, 7 cols
            Console.WriteLine("Goal is to predict iris species from sepal length,");
            Console.WriteLine("sepal width, petal length, petal width");
            Console.WriteLine("Setosa = (1,0,0), vericolor = (0,1,0), virginica = (0,0,1)");
            Console.WriteLine("\nThe 150-item data set is:\n");
            ShowMatrix(allData, 4, 1, true);

             double[][] trainData = null;
            double[][] testData = null;
            double trainPct = 0.80;
            int splitSeed = 1;
            Console.WriteLine("Splitting data into 80% train, 20% test");
            SplitData(allData, trainPct, splitSeed, out trainData, out testData);
            Console.WriteLine("\nThe training data is:\n");     
            ShowMatrix(trainData, 4, 1, true);
            Console.WriteLine("The test data is:\n");     
            ShowMatrix(testData, 3, 1, true);

            Console.WriteLine("\nBeginning training");
            double[] bestWeights = nn.Train(trainData, popSize, maxGeneration, exitError,
            mutateRate, mutateChange, tau);
            Console.WriteLine("Training complete");
            Console.WriteLine("\nFinal weights and bias values:");
            ShowVector(bestWeights, 10, 3, true);

            

             nn.SetWeights(bestWeights);
            double trainAcc = nn.Accuracy(trainData);
            Console.Write("\nAccuracy on training data = ");
            Console.WriteLine(trainAcc.ToString("F4"));

            double testAcc = nn.Accuracy(testData);
            Console.Write("\nAccuracy on test data = ");
            Console.WriteLine(testAcc.ToString("F4"));

            Console.ReadLine();
        }

                public static void ShowMatrix(double[][] matrix, int numRows,
            int decimals, bool indices)
        {
            int len = matrix.Length.ToString().Length;
            for (int i = 0; i < numRows; ++i)
            {
                if (indices == true)
                    Console.Write("[" + i.ToString().PadLeft(len) + "]  ");
                for (int j = 0; j < matrix[i].Length; ++j)
                {
                    double v = matrix[i][j];
                    if (v >= 0.0)
                        Console.Write(" "); // '+'
                    Console.Write(v.ToString("F" + decimals) + "  ");
                }
                Console.WriteLine("");
            }

            if (numRows < matrix.Length)
            {
                Console.WriteLine(". . .");
                int lastRow = matrix.Length - 1;
                if (indices == true)
                    Console.Write("[" + lastRow.ToString().PadLeft(len) + "]  ");
                for (int j = 0; j < matrix[lastRow].Length; ++j)
                {
                    double v = matrix[lastRow][j];
                    if (v >= 0.0)
                        Console.Write(" "); // '+'
                    Console.Write(v.ToString("F" + decimals) + "  ");
                }
            }
            Console.WriteLine("\n");
        }

        public static void ShowVector(double[] vector, int decimals,
            int lineLen, bool newLine)
        {
            for (int i = 0; i < vector.Length; ++i)
            {
                if (i > 0 && i % lineLen == 0) Console.WriteLine("");
                if (vector[i] >= 0) Console.Write(" ");
                Console.Write(vector[i].ToString("F" + decimals) + " ");
            }
            if (newLine == true)
                Console.WriteLine("");
        }
    }
      static void SplitData(double[][] allData, double trainPct,
            int seed, out double[][] trainData, out double[][] testData)
        {
            Random rnd = new Random(seed);
            int totRows = allData.Length;
            int numTrainRows = (int)(totRows * trainPct); // usually 0.80
            int numTestRows = totRows - numTrainRows;
            trainData = new double[numTrainRows][];
            testData = new double[numTestRows][];

            double[][] copy = new double[allData.Length][]; // ref copy of data
            for (int i = 0; i < copy.Length; ++i)
                copy[i] = allData[i];

            for (int i = 0; i < copy.Length; ++i) // scramble order of copy
            {
                int r = rnd.Next(i, copy.Length); // use Fisher-Yates
                double[] tmp = copy[r];
                copy[r] = copy[i];
                copy[i] = tmp;
            }
            for (int i = 0; i < numTrainRows; ++i) // by ref
                trainData[i] = copy[i];

            for (int i = 0; i < numTestRows; ++i)
                testData[i] = copy[i + numTrainRows];
        } // SplitData

          static double[][] LoadData(string dataFile, int numRows, int numCols)
        {
            double[][] result = new double[numRows][];

            FileStream ifs = new FileStream(dataFile, FileMode.Open);
            StreamReader sr = new StreamReader(ifs);
            string line = "";
            string[] tokens = null;
            int i = 0;
            while ((line = sr.ReadLine()) != null)
            {
                if (line[0] != '/' && line[1] != '/') //allows for comments
                {
                    tokens = line.Split(',');
                    result[i] = new double[numCols];
                    for (int j = 0; j < numCols; ++j)
                    {
                        result[i][j] = double.Parse(tokens[j]);
                    }
                    ++i;
                }
            }
            sr.Dispose();
            ifs.Dispose();
            return result;
        }

 
    }

        public class Individual : IComparable<Individual>
    {
        public double[] chromosome; // represents a solution
        public double error; // smaller values are better for minimization

        private int numGenes; // problem dimension (numWeights)
        private double minGene; // smallest value for a chromosome cell
        private double maxGene;
        private double mutateRate; // used during reproduction by Mutate
        private double mutateChange; // used during reproduction

        static Random rnd = new Random(0); // used by ctor for random genes

        public Individual(int numGenes, double minGene, double maxGene,
            double mutateRate,  double mutateChange)
        {
            this.numGenes = numGenes;
            this.minGene = minGene;
            this.maxGene = maxGene;
            this.mutateRate = mutateRate;
            this.mutateChange = mutateChange;
            this.chromosome = new double[numGenes];
            for (int i = 0; i < this.chromosome.Length; ++i)
                this.chromosome[i] = (maxGene - minGene) * rnd.NextDouble() + minGene;
            // this.error supplied after calling ctor!
        }

        public int CompareTo(Individual other) // smallest error to largest
        {
            if (this.error < other.error) return -1;
            else if (this.error > other.error) return 1;
            else return 0;
        }

         private double MeanSquaredError(double[][] trainData,
            double[] weights)
        {
            // how far off are computed values from desired values
            this.SetWeights(weights);

            double[] xValues = new double[numInput]; // inputs
            double[] tValues = new double[numOutput]; // targets
            double sumSquaredError = 0.0;
            for (int i = 0; i < trainData.Length; ++i)
            {
                // assumes data has x-values followed by y-values
                Array.Copy(trainData[i], xValues, numInput);
                Array.Copy(trainData[i], numInput, tValues, 0,
                    numOutput);
                double[] yValues = this.ComputeYs(xValues);
                for (int j = 0; j < yValues.Length; ++j)
                    sumSquaredError += ((yValues[j] - tValues[j]) *
                                        (yValues[j] - tValues[j]));
            }
            return sumSquaredError / trainData.Length;
        }

        public double Accuracy(double[][] testData)
        {
            // percentage correct using 'winner-takes all'
            int numCorrect = 0;
            int numWrong = 0;
            double[] xValues = new double[numInput]; // inputs
            double[] tValues = new double[numOutput]; // targets
            double[] yValues; // computed outputs

            for (int i = 0; i < testData.Length; ++i)
            {
                Array.Copy(testData[i], xValues, numInput);
                Array.Copy(testData[i], numInput, tValues, 0,
                    numOutput);
                yValues = this.ComputeYs(xValues);

                int maxIndex = MaxIndex(yValues);

                if (tValues[maxIndex] == 1.0) // not so nice
                    ++numCorrect;
                else
                    ++numWrong;
            }
            return (numCorrect * 1.0) / (numCorrect + numWrong);
        }

        private static int MaxIndex(double[] vector)
        {
            int bigIndex = 0;
            double biggestVal = vector[0];
            for (int i = 0; i < vector.Length; ++i)
            {
                if (vector[i] > biggestVal)
                {
                    biggestVal = vector[i]; bigIndex = i;
                }
            }
            return bigIndex;
        }

      

       
}
