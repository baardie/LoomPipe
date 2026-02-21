using Xunit;
using System;
using System.Collections.Generic;
using System.Dynamic;

namespace LoomPipe.Engine.Tests
{
    public class TransformationParserTests
    {
        // ── Helpers ───────────────────────────────────────────────────────────

        private static IDictionary<string, object> Run(string expr, IDictionary<string, object>? seed = null)
        {
            IDictionary<string, object> rec = seed ?? new ExpandoObject();
            TransformationParser.Parse(expr)(rec);
            return rec;
        }

        private static IDictionary<string, object> Seed(params (string k, object v)[] fields)
        {
            IDictionary<string, object> rec = new ExpandoObject();
            foreach (var (k, v) in fields) rec[k] = v;
            return rec;
        }

        // ── Assignment expressions ────────────────────────────────────────────

        [Fact]
        public void Parse_ShouldHandleSimpleAssignment()
        {
            var r = Run("a = 'hello'");
            Assert.Equal("hello", r["a"]);
        }

        [Fact]
        public void Assign_CopyField()
        {
            var r = Run("b = a", Seed(("a", "test")));
            Assert.Equal("test", r["b"]);
        }

        [Fact]
        public void Parse_ShouldHandleFieldToFieldAssignment()
        {
            var r = Run("b = a", Seed(("a", "test")));
            Assert.Equal("test", r["b"]);
        }

        [Fact]
        public void Assign_FieldPlusLiteral()
        {
            var r = Run("b = a + ' world'", Seed(("a", "hello")));
            Assert.Equal("hello world", r["b"]);
        }

        [Fact]
        public void Assign_LiteralPlusField()
        {
            var r = Run("b = 'prefix_' + a", Seed(("a", "value")));
            Assert.Equal("prefix_value", r["b"]);
        }

        // ── Arithmetic ────────────────────────────────────────────────────────

        [Fact]
        public void Parse_ShouldHandleStringConcatenation()
        {
            var r = Run("c = a + b", Seed(("a", "hello"), ("b", " world")));
            Assert.Equal("hello world", r["c"]);
        }

        [Fact]
        public void Parse_ShouldHandleIntegerAddition()
        {
            var r = Run("c = a + b", Seed(("a", 10), ("b", 5)));
            Assert.Equal(15, r["c"]);
        }

        [Fact]
        public void Arith_IntSubtract()
        {
            var r = Run("c = a - b", Seed(("a", 10), ("b", 3)));
            Assert.Equal(7, r["c"]);
        }

        [Fact]
        public void Arith_IntMultiply()
        {
            var r = Run("c = a * b", Seed(("a", 6), ("b", 7)));
            Assert.Equal(42, r["c"]);
        }

        [Fact]
        public void Arith_DoubleDivide()
        {
            var r = Run("c = a / b", Seed(("a", 10.0), ("b", 4.0)));
            Assert.Equal(2.5, r["c"]);
        }

        [Fact]
        public void Arith_DivideByZero_ReturnsZero()
        {
            var r = Run("c = a / b", Seed(("a", 5.0), ("b", 0.0)));
            Assert.Equal(0.0, r["c"]);
        }

        // ── String functions ──────────────────────────────────────────────────

        [Fact]
        public void Fn_Upper_InPlace()
        {
            var r = Run("UPPER(name)", Seed(("name", "hello")));
            Assert.Equal("HELLO", r["name"]);
        }

        [Fact]
        public void Fn_Upper_Assigned()
        {
            var r = Run("big = UPPER(name)", Seed(("name", "hello")));
            Assert.Equal("HELLO", r["big"]);
            Assert.Equal("hello", r["name"]); // original unchanged
        }

        [Fact]
        public void Fn_Lower_InPlace()
        {
            var r = Run("LOWER(name)", Seed(("name", "WORLD")));
            Assert.Equal("world", r["name"]);
        }

        [Fact]
        public void Fn_Trim()
        {
            var r = Run("TRIM(s)", Seed(("s", "  hello  ")));
            Assert.Equal("hello", r["s"]);
        }

        [Fact]
        public void Fn_Ltrim()
        {
            var r = Run("LTRIM(s)", Seed(("s", "  hello  ")));
            Assert.Equal("hello  ", r["s"]);
        }

        [Fact]
        public void Fn_Rtrim()
        {
            var r = Run("RTRIM(s)", Seed(("s", "  hello  ")));
            Assert.Equal("  hello", r["s"]);
        }

        [Fact]
        public void Fn_Replace_InPlace()
        {
            var r = Run("REPLACE(s, foo, bar)", Seed(("s", "foo and foo")));
            Assert.Equal("bar and bar", r["s"]);
        }

        [Fact]
        public void Fn_Replace_Assigned()
        {
            var r = Run("t = REPLACE(s, foo, bar)", Seed(("s", "foo and foo")));
            Assert.Equal("bar and bar", r["t"]);
            Assert.Equal("foo and foo", r["s"]);
        }

        [Fact]
        public void Fn_RegexReplace()
        {
            var r = Run(@"REGEX_REPLACE(s, \d+, #)", Seed(("s", "abc123def456")));
            Assert.Equal("abc#def#", r["s"]);
        }

        [Fact]
        public void Fn_Reverse()
        {
            var r = Run("REVERSE(s)", Seed(("s", "abcde")));
            Assert.Equal("edcba", r["s"]);
        }

        [Fact]
        public void Fn_Left()
        {
            var r = Run("LEFT(s, 3)", Seed(("s", "hello")));
            Assert.Equal("hel", r["s"]);
        }

        [Fact]
        public void Fn_Left_ShortString_ReturnsAll()
        {
            var r = Run("LEFT(s, 10)", Seed(("s", "hi")));
            Assert.Equal("hi", r["s"]);
        }

        [Fact]
        public void Fn_Right()
        {
            var r = Run("RIGHT(s, 3)", Seed(("s", "hello")));
            Assert.Equal("llo", r["s"]);
        }

        [Fact]
        public void Fn_Substring_1Indexed()
        {
            var r = Run("SUBSTRING(s, 2, 3)", Seed(("s", "hello")));
            Assert.Equal("ell", r["s"]);
        }

        [Fact]
        public void Fn_Len()
        {
            var r = Run("n = LEN(s)", Seed(("s", "hello")));
            Assert.Equal(5, r["n"]);
        }

        [Fact]
        public void Fn_Length_Alias()
        {
            var r = Run("n = LENGTH(s)", Seed(("s", "hi")));
            Assert.Equal(2, r["n"]);
        }

        [Fact]
        public void Fn_PadLeft()
        {
            var r = Run("PAD_LEFT(s, 6, 0)", Seed(("s", "42")));
            Assert.Equal("000042", r["s"]);
        }

        [Fact]
        public void Fn_PadRight()
        {
            var r = Run("PAD_RIGHT(s, 5, x)", Seed(("s", "hi")));
            Assert.Equal("hixxx", r["s"]);
        }

        [Fact]
        public void Fn_Split()
        {
            var r = Run("part = SPLIT(s, -, 2)", Seed(("s", "a-b-c")));
            Assert.Equal("b", r["part"]);
        }

        [Fact]
        public void Fn_Normalize()
        {
            var r = Run("NORMALIZE(s)", Seed(("s", "  hello   world  ")));
            Assert.Equal("hello world", r["s"]);
        }

        [Fact]
        public void Fn_TitleCase()
        {
            var r = Run("TITLE_CASE(s)", Seed(("s", "hello world")));
            Assert.Equal("Hello World", r["s"]);
        }

        [Fact]
        public void Fn_Slug()
        {
            var r = Run("SLUG(s)", Seed(("s", "Hello World! 2024")));
            Assert.Equal("hello-world-2024", r["s"]);
        }

        [Fact]
        public void Fn_Slug_StripSpecialChars()
        {
            var r = Run("SLUG(s)", Seed(("s", "C# .NET Framework")));
            Assert.Equal("c-net-framework", r["s"]);
        }

        [Fact]
        public void Fn_Concat()
        {
            var r = Run("full = CONCAT(first, last)", Seed(("first", "John"), ("last", "Doe")));
            Assert.Equal("JohnDoe", r["full"]);
        }

        // ── Numeric functions ─────────────────────────────────────────────────

        [Fact]
        public void Fn_Round_TwoDecimals()
        {
            var r = Run("ROUND(n, 2)", Seed(("n", 3.14159)));
            Assert.Equal(3.14, r["n"]);
        }

        [Fact]
        public void Fn_Round_HalfAwayFromZero()
        {
            var r = Run("ROUND(n, 0)", Seed(("n", 2.5)));
            Assert.Equal(3.0, r["n"]);
        }

        [Fact]
        public void Fn_Ceil()
        {
            var r = Run("CEIL(n)", Seed(("n", 2.1)));
            Assert.Equal(3.0, r["n"]);
        }

        [Fact]
        public void Fn_Ceiling_Alias()
        {
            var r = Run("CEILING(n)", Seed(("n", 2.1)));
            Assert.Equal(3.0, r["n"]);
        }

        [Fact]
        public void Fn_Floor()
        {
            var r = Run("FLOOR(n)", Seed(("n", 2.9)));
            Assert.Equal(2.0, r["n"]);
        }

        [Fact]
        public void Fn_Abs_Negative()
        {
            var r = Run("ABS(n)", Seed(("n", -7.5)));
            Assert.Equal(7.5, r["n"]);
        }

        [Fact]
        public void Fn_Mod()
        {
            var r = Run("r = MOD(n, 3)", Seed(("n", 10.0)));
            Assert.Equal(1.0, r["r"]);
        }

        // ── Type conversion ───────────────────────────────────────────────────

        [Fact]
        public void Fn_ToInt_FromString()
        {
            var r = Run("TO_INT(n)", Seed(("n", "42")));
            Assert.Equal(42, r["n"]);
        }

        [Fact]
        public void Fn_ToInt_Invalid_ReturnsZero()
        {
            var r = Run("TO_INT(n)", Seed(("n", "abc")));
            Assert.Equal(0, r["n"]);
        }

        [Fact]
        public void Fn_ToFloat_FromString()
        {
            var r = Run("TO_FLOAT(n)", Seed(("n", "3.14")));
            Assert.Equal(3.14, r["n"]);
        }

        [Fact]
        public void Fn_ToString_FromInt()
        {
            var r = Run("s = TO_STRING(n)", Seed(("n", 99)));
            Assert.Equal("99", r["s"]);
        }

        [Fact]
        public void Fn_ToBool_TrueValues()
        {
            foreach (var v in new[] { "1", "true", "yes", "on", "TRUE", "Yes" })
            {
                var r = Run("b = TO_BOOL(s)", Seed(("s", v)));
                Assert.True((bool)r["b"], $"Expected true for '{v}'");
            }
        }

        [Fact]
        public void Fn_ToBool_False()
        {
            var r = Run("b = TO_BOOL(s)", Seed(("s", "0")));
            Assert.False((bool)r["b"]);
        }

        // ── Date / time ───────────────────────────────────────────────────────

        [Fact]
        public void Fn_Now_WritesIso8601()
        {
            var before = DateTime.UtcNow;
            var r = Run("NOW(ts)");
            var after = DateTime.UtcNow;
            var ts = DateTime.Parse((string)r["ts"]).ToUniversalTime();
            Assert.InRange(ts, before, after);
        }

        [Fact]
        public void Fn_Today_WritesDateString()
        {
            var r = Run("TODAY(d)");
            Assert.Matches(@"^\d{4}-\d{2}-\d{2}$", (string)r["d"]);
        }

        [Fact]
        public void Fn_FormatDate()
        {
            var r = Run("FORMAT_DATE(dt, yyyy/MM/dd)", Seed(("dt", "2024-06-15")));
            Assert.Equal("2024/06/15", r["dt"]);
        }

        [Fact]
        public void Fn_AddDays()
        {
            var r = Run("ADD_DAYS(dt, 7)", Seed(("dt", "2024-01-01T00:00:00Z")));
            var dt = DateTime.Parse((string)r["dt"]).ToUniversalTime();
            Assert.Equal(new DateTime(2024, 1, 8, 0, 0, 0, DateTimeKind.Utc), dt);
        }

        [Fact]
        public void Fn_AddDays_Negative()
        {
            var r = Run("ADD_DAYS(dt, -1)", Seed(("dt", "2024-01-02T00:00:00Z")));
            var dt = DateTime.Parse((string)r["dt"]).ToUniversalTime();
            Assert.Equal(new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc), dt);
        }

        [Fact]
        public void Fn_DateDiff()
        {
            var r = Run("diff = DATE_DIFF(start, end)",
                Seed(("start", "2024-01-01"), ("end", "2024-01-11")));
            Assert.Equal(10, r["diff"]);
        }

        [Fact]
        public void Fn_Year()
        {
            var r = Run("y = YEAR(dt)", Seed(("dt", "2024-06-15")));
            Assert.Equal(2024, r["y"]);
        }

        [Fact]
        public void Fn_Month()
        {
            var r = Run("m = MONTH(dt)", Seed(("dt", "2024-06-15")));
            Assert.Equal(6, r["m"]);
        }

        [Fact]
        public void Fn_Day()
        {
            var r = Run("d = DAY(dt)", Seed(("dt", "2024-06-15")));
            Assert.Equal(15, r["d"]);
        }

        // ── Null / conditional ────────────────────────────────────────────────

        [Fact]
        public void Fn_Coalesce_SkipsEmpty()
        {
            var r = Run("result = COALESCE(a, b)", Seed(("a", ""), ("b", "fallback")));
            Assert.Equal("fallback", r["result"]);
        }

        [Fact]
        public void Fn_Coalesce_ReturnsFirst()
        {
            var r = Run("result = COALESCE(a, b)", Seed(("a", "first"), ("b", "second")));
            Assert.Equal("first", r["result"]);
        }

        [Fact]
        public void Fn_Default_WhenFieldEmpty()
        {
            var r = Run("DEFAULT(name, unknown)", Seed(("name", "")));
            Assert.Equal("unknown", r["name"]);
        }

        [Fact]
        public void Fn_Default_WhenFieldPresent()
        {
            var r = Run("DEFAULT(name, unknown)", Seed(("name", "Alice")));
            Assert.Equal("Alice", r["name"]);
        }

        [Fact]
        public void Fn_NullIf_Matches_ReturnsNull()
        {
            var r = Run("NULLIF(status, N/A)", Seed(("status", "N/A")));
            Assert.Null(r["status"]);
        }

        [Fact]
        public void Fn_NullIf_NoMatch_ReturnsValue()
        {
            var r = Run("NULLIF(status, N/A)", Seed(("status", "Active")));
            Assert.Equal("Active", r["status"]);
        }

        // ── Encoding / hashing ────────────────────────────────────────────────

        [Fact]
        public void Fn_Md5_KnownHash()
        {
            var r = Run("MD5(s)", Seed(("s", "hello")));
            Assert.Equal("5d41402abc4b2a76b9719d911017c592", r["s"]);
        }

        [Fact]
        public void Fn_Sha256_KnownHash()
        {
            var r = Run("SHA256(s)", Seed(("s", "hello")));
            Assert.Equal("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", r["s"]);
        }

        [Fact]
        public void Fn_Base64_RoundTrip()
        {
            var r = Run("BASE64_ENCODE(s)", Seed(("s", "hello world")));
            Assert.Equal("aGVsbG8gd29ybGQ=", r["s"]);

            r = Run("BASE64_DECODE(s)", Seed(("s", "aGVsbG8gd29ybGQ=")));
            Assert.Equal("hello world", r["s"]);
        }

        [Fact]
        public void Fn_Base64Decode_InvalidInput_ReturnsEmpty()
        {
            var r = Run("BASE64_DECODE(s)", Seed(("s", "!!not-base64!!")));
            Assert.Equal(string.Empty, r["s"]);
        }

        [Fact]
        public void Fn_UrlEncode()
        {
            var r = Run("URL_ENCODE(s)", Seed(("s", "hello world&foo=bar")));
            Assert.Equal("hello%20world%26foo%3Dbar", r["s"]);
        }

        [Fact]
        public void Fn_UrlDecode()
        {
            var r = Run("URL_DECODE(s)", Seed(("s", "hello%20world")));
            Assert.Equal("hello world", r["s"]);
        }

        // ── Assigned form: source field untouched ─────────────────────────────

        [Fact]
        public void Fn_Assigned_DoesNotMutateSource()
        {
            var r = Run("upper = UPPER(name)", Seed(("name", "alice")));
            Assert.Equal("ALICE", r["upper"]);
            Assert.Equal("alice", r["name"]);
        }

        [Fact]
        public void Fn_Assigned_Substring()
        {
            var r = Run("code = SUBSTRING(s, 1, 3)", Seed(("s", "ABCDEF")));
            Assert.Equal("ABC", r["code"]);
            Assert.Equal("ABCDEF", r["s"]);
        }

        [Fact]
        public void Fn_Assigned_Len()
        {
            var r = Run("n = LEN(s)", Seed(("s", "hello")));
            Assert.Equal(5, r["n"]);
            Assert.Equal("hello", r["s"]);
        }

        // ── Error cases ───────────────────────────────────────────────────────

        [Fact]
        public void Unknown_Function_Throws()
        {
            Assert.Throws<NotSupportedException>(() =>
                TransformationParser.Parse("FOOBAR(x)"));
        }

        [Fact]
        public void Unknown_Expression_Throws()
        {
            Assert.Throws<NotSupportedException>(() =>
                TransformationParser.Parse("???"));
        }
    }
}
