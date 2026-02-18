using Xunit;
using System.Collections.Generic;
using System.Linq;
using System;

namespace LoomPipe.Engine.Tests
{
    public class AutomapHelperTests
    {
        [Fact]
        public void AutomapFields_ShouldReturnExactMatch()
        {
            // Arrange
            var sourceFields = new List<string> { "FirstName", "LastName", "Email" };
            var destFields = new List<string> { "Email", "FirstName", "LastName" };

            // Act
            var result = AutomapHelper.AutomapFields(sourceFields, destFields);

            // Assert
            Assert.Equal(3, result.Count);
            Assert.Contains(result, m => m.source == "FirstName" && m.dest == "FirstName" && m.score == 1.0);
            Assert.Contains(result, m => m.source == "LastName" && m.dest == "LastName" && m.score == 1.0);
            Assert.Contains(result, m => m.source == "Email" && m.dest == "Email" && m.score == 1.0);
        }

        [Fact]
        public void AutomapFields_ShouldReturnFuzzyMatch()
        {
            // Arrange
            var sourceFields = new List<string> { "FName", "LName", "EmailAddress" };
            var destFields = new List<string> { "FirstName", "LastName", "Email" };

            // Act
            var result = AutomapHelper.AutomapFields(sourceFields, destFields);

            // Assert
            foreach (var r in result)
            {
                Console.WriteLine($"Source: {r.source}, Dest: {r.dest}, Score: {r.score}");
            }
            Assert.Equal(3, result.Count);
            Assert.Contains(result, m => m.source == "FName" && m.dest == "FirstName" && m.score > 0.5);
            Assert.Contains(result, m => m.source == "LName" && m.dest == "LastName" && m.score > 0.5);
            Assert.Contains(result, m => m.source == "EmailAddress" && m.dest == "Email" && m.score > 0.5);
        }

        [Fact]
        public void LevenshteinSimilarity_ShouldReturnCorrectScore()
        {
            // Arrange
            var a = "FName";
            var b = "FirstName";

            // Act
            var score = AutomapHelper.LevenshteinSimilarity(a, b);
            Console.WriteLine($"Levenshtein score for '{a}' and '{b}': {score}");

            // Assert
            Assert.True(score > 0.5);
        }

        [Fact]
        public void LevenshteinSimilarity_ShouldReturnCorrectScore_ForEmail()
        {
            // Arrange
            var a = "EmailAddress";
            var b = "Email";

            // Act
            var score = AutomapHelper.LevenshteinSimilarity(a, b);
            Console.WriteLine($"Levenshtein score for '{a}' and '{b}': {score}");

            // Assert
            Assert.True(score > 0.5);
        }

        [Fact]
        public void AutomapFields_ShouldNotReturnLowQualityMatch()
        {
            // Arrange
            var sourceFields = new List<string> { "A", "B", "C" };
            var destFields = new List<string> { "X", "Y", "Z" };

            // Act
            var result = AutomapHelper.AutomapFields(sourceFields, destFields);

            // Assert
            Assert.Empty(result);
        }
    }
}
