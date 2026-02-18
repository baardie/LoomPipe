using System;
using System.Collections.Generic;
using System.Linq;

namespace LoomPipe.Engine
{
    /// <summary>
    /// Provides automap logic for field mapping: Level 1 (exact), Level 2 (fuzzy/Levenshtein), Level 3 (manual override).
    /// </summary>
    public static class AutomapHelper
    {
        public static List<(string source, string dest, double score)> AutomapFields(IEnumerable<string> sourceFields, IEnumerable<string> destFields)
        {
            var results = new List<(string, string, double)>();
            var remainingDestFields = destFields.ToList();

            foreach (var src in sourceFields)
            {
                (string? dest, double score) bestMatch = (null, 0.0);

                foreach (var dst in remainingDestFields)
                {
                    double score = GetAutomapScore(src, dst);
                    if (score > bestMatch.score)
                    {
                        bestMatch = (dst, score);
                    }
                }

                if (bestMatch.dest != null && (bestMatch.score == 1.0 || bestMatch.score > 0.5))
                {
                    results.Add((src, bestMatch.dest, bestMatch.score));
                    remainingDestFields.Remove(bestMatch.dest);
                }
            }
            return results;
        }

        public static double GetAutomapScore(string a, string b)
        {
            if (string.Equals(a, b, StringComparison.OrdinalIgnoreCase))
                return 1.0;
            return LevenshteinSimilarity(a, b);
        }

        // Levenshtein similarity: 1 - (distance / (length of both strings))
        public static double LevenshteinSimilarity(string a, string b)
        {
            int distance = LevenshteinDistance(a, b);
            int totalLen = a.Length + b.Length;
            if (totalLen == 0) return 1.0;
            return 1.0 - (double)distance / totalLen;
        }

        // Standard Levenshtein distance
        public static int LevenshteinDistance(string a, string b)
        {
            int[,] d = new int[a.Length + 1, b.Length + 1];
            for (int i = 0; i <= a.Length; i++) d[i, 0] = i;
            for (int j = 0; j <= b.Length; j++) d[0, j] = j;
            for (int i = 1; i <= a.Length; i++)
            {
                for (int j = 1; j <= b.Length; j++)
                {
                    int cost = (a[i - 1] == b[j - 1]) ? 0 : 1;
                    d[i, j] = Math.Min(
                        Math.Min(d[i - 1, j] + 1, d[i, j - 1] + 1),
                        d[i - 1, j - 1] + cost);
                }
            }
            return d[a.Length, b.Length];
        }
    }
}
