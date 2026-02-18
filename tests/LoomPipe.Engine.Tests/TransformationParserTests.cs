using Xunit;
using System.Collections.Generic;
using System.Dynamic;

namespace LoomPipe.Engine.Tests
{
    public class TransformationParserTests
    {
        [Fact]
        public void Parse_ShouldHandleSimpleAssignment()
        {
            // Arrange
            var transformString = "a = 'hello'";
            var func = TransformationParser.Parse(transformString);
            IDictionary<string, object> data = new ExpandoObject();

            // Act
            func(data);

            // Assert
            Assert.True(data.ContainsKey("a"));
            Assert.Equal("hello", data["a"]);
        }

        [Fact]
        public void Parse_ShouldHandleStringConcatenation()
        {
            // Arrange
            var transformString = "c = a + b";
            var func = TransformationParser.Parse(transformString);
            IDictionary<string, object> data = new ExpandoObject();
            data["a"] = "hello";
            data["b"] = " world";

            // Act
            func(data);

            // Assert
            Assert.True(data.ContainsKey("c"));
            Assert.Equal("hello world", data["c"]);
        }

        [Fact]
        public void Parse_ShouldHandleIntegerAddition()
        {
            // Arrange
            var transformString = "c = a + b";
            var func = TransformationParser.Parse(transformString);
            IDictionary<string, object> data = new ExpandoObject();
            data["a"] = 10;
            data["b"] = 5;

            // Act
            func(data);

            // Assert
            Assert.True(data.ContainsKey("c"));
            Assert.Equal(15, data["c"]);
        }

        [Fact]
        public void Parse_ShouldHandleFieldToFieldAssignment()
        {
            // Arrange
            var transformString = "b = a";
            var func = TransformationParser.Parse(transformString);
            IDictionary<string, object> data = new ExpandoObject();
            data["a"] = "test";

            // Act
            func(data);

            // Assert
            Assert.True(data.ContainsKey("b"));
            Assert.Equal("test", data["b"]);
        }
    }
}
