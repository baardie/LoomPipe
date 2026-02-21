using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace LoomPipe.Engine
{
    /// <summary>
    /// Parses and compiles pipeline transformation expressions into executable delegates.
    ///
    /// ── Assignment expressions (dest = expr) ─────────────────────────────────
    ///   dest = 'literal'              set dest to a string constant
    ///   dest = field                  copy field value to dest
    ///   dest = fieldA + fieldB        add numbers; concatenate strings
    ///   dest = fieldA - fieldB        subtract numbers
    ///   dest = fieldA * fieldB        multiply numbers
    ///   dest = fieldA / fieldB        divide numbers (0 on divide-by-zero)
    ///   dest = field + 'literal'      append literal to field value
    ///   dest = 'literal' + field      prepend literal to field value
    ///   dest = FUNC(args...)          apply function to args, write result to dest
    ///
    /// ── In-place function call (first arg is both source and dest) ────────────
    ///   FUNC(field, args...)          equivalent to  field = FUNC(field, args...)
    ///
    /// ── String functions ──────────────────────────────────────────────────────
    ///   UPPER(field)                  uppercase
    ///   LOWER(field)                  lowercase
    ///   TRIM(field)                   strip leading + trailing whitespace
    ///   LTRIM(field)                  strip leading whitespace
    ///   RTRIM(field)                  strip trailing whitespace
    ///   REPLACE(field, old, new)      replace all occurrences of old with new
    ///   REGEX_REPLACE(field, pat, repl)  regex replace
    ///   REVERSE(field)                reverse characters
    ///   LEFT(field, n)                first n characters
    ///   RIGHT(field, n)               last n characters
    ///   SUBSTRING(field, start, len)  substring (1-indexed start)
    ///   LEN(field) / LENGTH(field)    character count → stores integer
    ///   PAD_LEFT(field, width, char)  left-pad to width with char
    ///   PAD_RIGHT(field, width, char) right-pad to width with char
    ///   SPLIT(field, delim, index)    split by delimiter, return part at 1-indexed position
    ///   NORMALIZE(field)              collapse runs of whitespace to a single space and trim
    ///   TITLE_CASE(field)             Title Case
    ///   SLUG(field)                   url-safe slug (lowercase, hyphens, no special chars)
    ///   CONCAT(field1, field2, ...)   concatenate 2+ fields
    ///
    /// ── Numeric functions ─────────────────────────────────────────────────────
    ///   ROUND(field, decimals)        round to N decimal places
    ///   CEIL(field) / CEILING(field)  ceiling
    ///   FLOOR(field)                  floor
    ///   ABS(field)                    absolute value
    ///   MOD(field, divisor)           modulo
    ///
    /// ── Type-conversion functions ─────────────────────────────────────────────
    ///   TO_INT(field)                 parse as integer (0 on failure)
    ///   TO_FLOAT(field)               parse as double  (0.0 on failure)
    ///   TO_STRING(field)              coerce to string
    ///   TO_BOOL(field)                "1"/"true"/"yes"/"on" → true, else false
    ///
    /// ── Date / time functions ─────────────────────────────────────────────────
    ///   NOW(dest)                     write current UTC ISO-8601 datetime to dest
    ///   TODAY(dest)                   write current UTC date (yyyy-MM-dd) to dest
    ///   FORMAT_DATE(field, format)    reformat date string using .NET format codes
    ///   ADD_DAYS(field, n)            add N days to a date (float OK, e.g. -1)
    ///   DATE_DIFF(field1, field2)     integer days between two dates (field2 − field1)
    ///   YEAR(field)                   extract year as integer
    ///   MONTH(field)                  extract month as integer
    ///   DAY(field)                    extract day-of-month as integer
    ///
    /// ── Null / conditional functions ─────────────────────────────────────────
    ///   COALESCE(field1, field2, ...) first non-null, non-empty value
    ///   DEFAULT(field, fallback)      field value if non-empty, otherwise fallback literal
    ///   NULLIF(field, value)          null if field equals value, otherwise field value
    ///
    /// ── Encoding / hashing functions ──────────────────────────────────────────
    ///   MD5(field)                    MD5 hex digest (lowercase)
    ///   SHA256(field)                 SHA-256 hex digest (lowercase)
    ///   BASE64_ENCODE(field)          Base-64 encode UTF-8 bytes
    ///   BASE64_DECODE(field)          Base-64 decode to UTF-8 string (empty on error)
    ///   URL_ENCODE(field)             percent-encode (RFC 3986)
    ///   URL_DECODE(field)             percent-decode
    /// </summary>
    public static class TransformationParser
    {
        // ── Assignment expression patterns: dest = expr ───────────────────────

        private static readonly Regex AssignLiteralRegex    = new(@"^(\w+)\s*=\s*'([^']*)'$",               RegexOptions.Compiled);
        private static readonly Regex FieldPlusLiteralRegex = new(@"^(\w+)\s*=\s*(\w+)\s*\+\s*'([^']*)'$", RegexOptions.Compiled);
        private static readonly Regex LiteralPlusFieldRegex = new(@"^(\w+)\s*=\s*'([^']*)'\s*\+\s*(\w+)$", RegexOptions.Compiled);
        private static readonly Regex FieldPlusFieldRegex   = new(@"^(\w+)\s*=\s*(\w+)\s*\+\s*(\w+)$",     RegexOptions.Compiled);
        private static readonly Regex FieldMinusFieldRegex  = new(@"^(\w+)\s*=\s*(\w+)\s*-\s*(\w+)$",      RegexOptions.Compiled);
        private static readonly Regex FieldMulFieldRegex    = new(@"^(\w+)\s*=\s*(\w+)\s*\*\s*(\w+)$",     RegexOptions.Compiled);
        private static readonly Regex FieldDivFieldRegex    = new(@"^(\w+)\s*=\s*(\w+)\s*/\s*(\w+)$",      RegexOptions.Compiled);
        private static readonly Regex AssignFieldRegex      = new(@"^(\w+)\s*=\s*(\w+)$",                   RegexOptions.Compiled);

        // ── Function patterns ─────────────────────────────────────────────────

        // dest = FUNC(args)  — assigns function result to a different field
        private static readonly Regex AssignFunctionRegex = new(@"^(\w+)\s*=\s*(\w+)\(\s*([^)]*)\s*\)$", RegexOptions.Compiled);
        // FUNC(args)         — in-place; first arg is both source and destination
        private static readonly Regex FunctionRegex       = new(@"^(\w+)\(\s*([^)]*)\s*\)$",             RegexOptions.Compiled);

        // ── Slug helpers (compiled once) ──────────────────────────────────────

        private static readonly Regex SlugStripRegex = new(@"[^a-z0-9\s-]", RegexOptions.Compiled);
        private static readonly Regex SlugCollapseRegex = new(@"[\s-]+",     RegexOptions.Compiled);
        private static readonly Regex NormalizeWsRegex  = new(@"\s+",        RegexOptions.Compiled);

        // ── Entry point ───────────────────────────────────────────────────────

        public static Action<IDictionary<string, object>> Parse(string transformation)
        {
            var t = transformation.Trim();

            // dest = 'literal'
            var m = AssignLiteralRegex.Match(t);
            if (m.Success)
            {
                var dest    = m.Groups[1].Value;
                var literal = m.Groups[2].Value;
                return record => record[dest] = literal;
            }

            // dest = field + 'literal'
            m = FieldPlusLiteralRegex.Match(t);
            if (m.Success)
            {
                var dest    = m.Groups[1].Value;
                var src     = m.Groups[2].Value;
                var literal = m.Groups[3].Value;
                return record => record[dest] = Str(record, src) + literal;
            }

            // dest = 'literal' + field
            m = LiteralPlusFieldRegex.Match(t);
            if (m.Success)
            {
                var dest    = m.Groups[1].Value;
                var literal = m.Groups[2].Value;
                var src     = m.Groups[3].Value;
                return record => record[dest] = literal + Str(record, src);
            }

            // dest = fieldA + fieldB  (numeric add or string concat)
            m = FieldPlusFieldRegex.Match(t);
            if (m.Success)
            {
                var (dest, a, b) = (m.Groups[1].Value, m.Groups[2].Value, m.Groups[3].Value);
                return record => { record.TryGetValue(a, out var va); record.TryGetValue(b, out var vb); record[dest] = ArithOp(va, vb, "+"); };
            }

            // dest = fieldA - fieldB
            m = FieldMinusFieldRegex.Match(t);
            if (m.Success)
            {
                var (dest, a, b) = (m.Groups[1].Value, m.Groups[2].Value, m.Groups[3].Value);
                return record => { record.TryGetValue(a, out var va); record.TryGetValue(b, out var vb); record[dest] = ArithOp(va, vb, "-"); };
            }

            // dest = fieldA * fieldB
            m = FieldMulFieldRegex.Match(t);
            if (m.Success)
            {
                var (dest, a, b) = (m.Groups[1].Value, m.Groups[2].Value, m.Groups[3].Value);
                return record => { record.TryGetValue(a, out var va); record.TryGetValue(b, out var vb); record[dest] = ArithOp(va, vb, "*"); };
            }

            // dest = fieldA / fieldB
            m = FieldDivFieldRegex.Match(t);
            if (m.Success)
            {
                var (dest, a, b) = (m.Groups[1].Value, m.Groups[2].Value, m.Groups[3].Value);
                return record => { record.TryGetValue(a, out var va); record.TryGetValue(b, out var vb); record[dest] = ArithOp(va, vb, "/"); };
            }

            // dest = FUNC(args)
            m = AssignFunctionRegex.Match(t);
            if (m.Success)
            {
                var dest = m.Groups[1].Value;
                var func = m.Groups[2].Value.ToUpperInvariant();
                var args = ParseArgs(m.Groups[3].Value);
                return BuildFunction(func, dest, args);
            }

            // dest = srcField (copy) — must come after all other dest = … patterns
            m = AssignFieldRegex.Match(t);
            if (m.Success)
            {
                var dest = m.Groups[1].Value;
                var src  = m.Groups[2].Value;
                return record => record[dest] = record.TryGetValue(src, out var v) ? v : null!;
            }

            // FUNC(args) — in-place (first arg is both source and destination)
            m = FunctionRegex.Match(t);
            if (m.Success)
            {
                var func = m.Groups[1].Value.ToUpperInvariant();
                var args = ParseArgs(m.Groups[2].Value);
                if (args.Length == 0)
                    throw new ArgumentException($"Function '{func}' requires at least one argument.");
                var dest = args[0];
                return BuildFunction(func, dest, args);
            }

            throw new NotSupportedException($"Transformation '{transformation}' is not supported.");
        }

        // ── Function dispatcher ───────────────────────────────────────────────

        private static Action<IDictionary<string, object>> BuildFunction(
            string func, string dest, string[] args) => func switch
        {
            // String
            "UPPER"         => Fn_Upper(dest, args),
            "LOWER"         => Fn_Lower(dest, args),
            "TRIM"          => Fn_Trim(dest, args),
            "LTRIM"         => Fn_Ltrim(dest, args),
            "RTRIM"         => Fn_Rtrim(dest, args),
            "REPLACE"       => Fn_Replace(dest, args),
            "REGEX_REPLACE" => Fn_RegexReplace(dest, args),
            "REVERSE"       => Fn_Reverse(dest, args),
            "LEFT"          => Fn_Left(dest, args),
            "RIGHT"         => Fn_Right(dest, args),
            "SUBSTRING"     => Fn_Substring(dest, args),
            "LEN"           => Fn_Len(dest, args),
            "LENGTH"        => Fn_Len(dest, args),
            "PAD_LEFT"      => Fn_PadLeft(dest, args),
            "PAD_RIGHT"     => Fn_PadRight(dest, args),
            "SPLIT"         => Fn_Split(dest, args),
            "NORMALIZE"     => Fn_Normalize(dest, args),
            "TITLE_CASE"    => Fn_TitleCase(dest, args),
            "SLUG"          => Fn_Slug(dest, args),
            "CONCAT"        => Fn_Concat(dest, args),
            // Numeric
            "ROUND"         => Fn_Round(dest, args),
            "CEIL"          => Fn_Ceil(dest, args),
            "CEILING"       => Fn_Ceil(dest, args),
            "FLOOR"         => Fn_Floor(dest, args),
            "ABS"           => Fn_Abs(dest, args),
            "MOD"           => Fn_Mod(dest, args),
            // Type conversion
            "TO_INT"        => Fn_ToInt(dest, args),
            "TO_FLOAT"      => Fn_ToFloat(dest, args),
            "TO_STRING"     => Fn_ToString(dest, args),
            "TO_BOOL"       => Fn_ToBool(dest, args),
            // Date / time
            "NOW"           => Fn_Now(dest),
            "TODAY"         => Fn_Today(dest),
            "FORMAT_DATE"   => Fn_FormatDate(dest, args),
            "ADD_DAYS"      => Fn_AddDays(dest, args),
            "DATE_DIFF"     => Fn_DateDiff(dest, args),
            "YEAR"          => Fn_Year(dest, args),
            "MONTH"         => Fn_Month(dest, args),
            "DAY"           => Fn_Day(dest, args),
            // Null / conditional
            "COALESCE"      => Fn_Coalesce(dest, args),
            "DEFAULT"       => Fn_Default(dest, args),
            "NULLIF"        => Fn_NullIf(dest, args),
            // Encoding / hashing
            "MD5"           => Fn_Md5(dest, args),
            "SHA256"        => Fn_Sha256(dest, args),
            "BASE64_ENCODE" => Fn_Base64Encode(dest, args),
            "BASE64_DECODE" => Fn_Base64Decode(dest, args),
            "URL_ENCODE"    => Fn_UrlEncode(dest, args),
            "URL_DECODE"    => Fn_UrlDecode(dest, args),
            _ => throw new NotSupportedException($"Function '{func}' is not supported.")
        };

        // ── Helpers ───────────────────────────────────────────────────────────

        private static string[] ParseArgs(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) return Array.Empty<string>();
            var parts = raw.Split(',');
            for (int i = 0; i < parts.Length; i++)
                parts[i] = parts[i].Trim().Trim('\'');
            return parts;
        }

        private static string Str(IDictionary<string, object> r, string field)
            => r.TryGetValue(field, out var v) ? v?.ToString() ?? string.Empty : string.Empty;

        private static double ToDouble(object? v) => v switch
        {
            int     i   => i,
            long    l   => l,
            float   f   => f,
            double  d   => d,
            decimal dec => (double)dec,
            _           => double.TryParse(v?.ToString(),
                               NumberStyles.Any, CultureInfo.InvariantCulture, out var r) ? r : 0.0
        };

        private static bool IsNumeric(object? v, out double d)
        {
            d = 0;
            switch (v)
            {
                case int    i:   d = i;          return true;
                case long   l:   d = l;          return true;
                case float  f:   d = f;          return true;
                case double dv:  d = dv;         return true;
                case decimal dc: d = (double)dc; return true;
                default:
                    return double.TryParse(v?.ToString(),
                        NumberStyles.Any, CultureInfo.InvariantCulture, out d);
            }
        }

        private static object ArithOp(object? a, object? b, string op)
        {
            // For + fall back to string concat when either value is non-numeric
            if (op == "+" && (!IsNumeric(a, out _) || !IsNumeric(b, out _)))
                return (a?.ToString() ?? string.Empty) + (b?.ToString() ?? string.Empty);

            IsNumeric(a, out var da);
            IsNumeric(b, out var db);

            // Preserve integer type when both operands are ints (except division)
            if (a is int ia && b is int ib && op != "/")
            {
                return op switch
                {
                    "+" => (object)(ia + ib),
                    "-" => (object)(ia - ib),
                    "*" => (object)(ia * ib),
                    _   => (object)0
                };
            }

            return op switch
            {
                "+" => (object)(da + db),
                "-" => (object)(da - db),
                "*" => (object)(da * db),
                "/" => db == 0 ? (object)0.0 : (object)(da / db),
                _   => throw new NotSupportedException($"Arithmetic operator '{op}' is not supported.")
            };
        }

        private static void Need(string[] args, int count, string func)
        {
            if (args.Length < count)
                throw new ArgumentException($"{func} requires {count} argument(s) but got {args.Length}.");
        }

        private static DateTime? TryParseDate(string s)
            => DateTime.TryParse(s, null, DateTimeStyles.RoundtripKind, out var dt) ? dt : null;

        // ── String functions ──────────────────────────────────────────────────

        private static Action<IDictionary<string, object>> Fn_Upper(string dest, string[] args)
        {
            Need(args, 1, "UPPER");
            var src = args[0];
            return r => r[dest] = Str(r, src).ToUpperInvariant();
        }

        private static Action<IDictionary<string, object>> Fn_Lower(string dest, string[] args)
        {
            Need(args, 1, "LOWER");
            var src = args[0];
            return r => r[dest] = Str(r, src).ToLowerInvariant();
        }

        private static Action<IDictionary<string, object>> Fn_Trim(string dest, string[] args)
        {
            Need(args, 1, "TRIM");
            var src = args[0];
            return r => r[dest] = Str(r, src).Trim();
        }

        private static Action<IDictionary<string, object>> Fn_Ltrim(string dest, string[] args)
        {
            Need(args, 1, "LTRIM");
            var src = args[0];
            return r => r[dest] = Str(r, src).TrimStart();
        }

        private static Action<IDictionary<string, object>> Fn_Rtrim(string dest, string[] args)
        {
            Need(args, 1, "RTRIM");
            var src = args[0];
            return r => r[dest] = Str(r, src).TrimEnd();
        }

        private static Action<IDictionary<string, object>> Fn_Replace(string dest, string[] args)
        {
            Need(args, 3, "REPLACE");
            var (src, oldVal, newVal) = (args[0], args[1], args[2]);
            return r => r[dest] = Str(r, src).Replace(oldVal, newVal);
        }

        private static Action<IDictionary<string, object>> Fn_RegexReplace(string dest, string[] args)
        {
            Need(args, 3, "REGEX_REPLACE");
            var src  = args[0];
            var rx   = new Regex(args[1]);
            var repl = args[2];
            return r => r[dest] = rx.Replace(Str(r, src), repl);
        }

        private static Action<IDictionary<string, object>> Fn_Reverse(string dest, string[] args)
        {
            Need(args, 1, "REVERSE");
            var src = args[0];
            return r =>
            {
                var arr = Str(r, src).ToCharArray();
                Array.Reverse(arr);
                r[dest] = new string(arr);
            };
        }

        private static Action<IDictionary<string, object>> Fn_Left(string dest, string[] args)
        {
            Need(args, 2, "LEFT");
            var src = args[0];
            var n   = int.Parse(args[1]);
            return r =>
            {
                var val = Str(r, src);
                r[dest] = n >= val.Length ? val : val.Substring(0, n);
            };
        }

        private static Action<IDictionary<string, object>> Fn_Right(string dest, string[] args)
        {
            Need(args, 2, "RIGHT");
            var src = args[0];
            var n   = int.Parse(args[1]);
            return r =>
            {
                var val = Str(r, src);
                r[dest] = n >= val.Length ? val : val.Substring(val.Length - n);
            };
        }

        private static Action<IDictionary<string, object>> Fn_Substring(string dest, string[] args)
        {
            Need(args, 3, "SUBSTRING");
            var src    = args[0];
            var start  = Math.Max(0, int.Parse(args[1]) - 1); // user-facing 1-indexed
            var length = int.Parse(args[2]);
            return r =>
            {
                var val = Str(r, src);
                if (start >= val.Length) { r[dest] = string.Empty; return; }
                r[dest] = val.Substring(start, Math.Min(length, val.Length - start));
            };
        }

        private static Action<IDictionary<string, object>> Fn_Len(string dest, string[] args)
        {
            Need(args, 1, "LEN");
            var src = args[0];
            return r => r[dest] = Str(r, src).Length;
        }

        private static Action<IDictionary<string, object>> Fn_PadLeft(string dest, string[] args)
        {
            Need(args, 3, "PAD_LEFT");
            var src  = args[0];
            var width = int.Parse(args[1]);
            var pad  = args[2].Length > 0 ? args[2][0] : ' ';
            return r => r[dest] = Str(r, src).PadLeft(width, pad);
        }

        private static Action<IDictionary<string, object>> Fn_PadRight(string dest, string[] args)
        {
            Need(args, 3, "PAD_RIGHT");
            var src   = args[0];
            var width = int.Parse(args[1]);
            var pad   = args[2].Length > 0 ? args[2][0] : ' ';
            return r => r[dest] = Str(r, src).PadRight(width, pad);
        }

        private static Action<IDictionary<string, object>> Fn_Split(string dest, string[] args)
        {
            Need(args, 3, "SPLIT");
            var src   = args[0];
            var delim = args[1];
            var idx   = int.Parse(args[2]) - 1; // 1-indexed
            return r =>
            {
                var parts = Str(r, src).Split(new[] { delim }, StringSplitOptions.None);
                r[dest] = idx >= 0 && idx < parts.Length ? parts[idx] : string.Empty;
            };
        }

        private static Action<IDictionary<string, object>> Fn_Normalize(string dest, string[] args)
        {
            Need(args, 1, "NORMALIZE");
            var src = args[0];
            return r => r[dest] = NormalizeWsRegex.Replace(Str(r, src).Trim(), " ");
        }

        private static Action<IDictionary<string, object>> Fn_TitleCase(string dest, string[] args)
        {
            Need(args, 1, "TITLE_CASE");
            var src = args[0];
            return r => r[dest] = CultureInfo.InvariantCulture.TextInfo
                                    .ToTitleCase(Str(r, src).ToLowerInvariant());
        }

        private static Action<IDictionary<string, object>> Fn_Slug(string dest, string[] args)
        {
            Need(args, 1, "SLUG");
            var src = args[0];
            return r =>
            {
                var val = Str(r, src).ToLowerInvariant();
                val = SlugStripRegex.Replace(val, string.Empty);
                val = SlugCollapseRegex.Replace(val, "-");
                r[dest] = val.Trim('-');
            };
        }

        private static Action<IDictionary<string, object>> Fn_Concat(string dest, string[] args)
        {
            if (args.Length < 2) throw new ArgumentException("CONCAT requires at least 2 arguments.");
            return r => r[dest] = string.Concat(Array.ConvertAll(args, f => Str(r, f)));
        }

        // ── Numeric functions ─────────────────────────────────────────────────

        private static Action<IDictionary<string, object>> Fn_Round(string dest, string[] args)
        {
            Need(args, 2, "ROUND");
            var src      = args[0];
            var decimals = int.Parse(args[1]);
            return r => r[dest] = Math.Round(
                ToDouble(r.TryGetValue(src, out var v) ? v : null),
                decimals,
                MidpointRounding.AwayFromZero);
        }

        private static Action<IDictionary<string, object>> Fn_Ceil(string dest, string[] args)
        {
            Need(args, 1, "CEIL");
            var src = args[0];
            return r => r[dest] = Math.Ceiling(ToDouble(r.TryGetValue(src, out var v) ? v : null));
        }

        private static Action<IDictionary<string, object>> Fn_Floor(string dest, string[] args)
        {
            Need(args, 1, "FLOOR");
            var src = args[0];
            return r => r[dest] = Math.Floor(ToDouble(r.TryGetValue(src, out var v) ? v : null));
        }

        private static Action<IDictionary<string, object>> Fn_Abs(string dest, string[] args)
        {
            Need(args, 1, "ABS");
            var src = args[0];
            return r => r[dest] = Math.Abs(ToDouble(r.TryGetValue(src, out var v) ? v : null));
        }

        private static Action<IDictionary<string, object>> Fn_Mod(string dest, string[] args)
        {
            Need(args, 2, "MOD");
            var src = args[0];
            var div = double.Parse(args[1], CultureInfo.InvariantCulture);
            return r =>
            {
                var d = ToDouble(r.TryGetValue(src, out var v) ? v : null);
                r[dest] = div == 0 ? 0.0 : d % div;
            };
        }

        // ── Type conversion ───────────────────────────────────────────────────

        private static Action<IDictionary<string, object>> Fn_ToInt(string dest, string[] args)
        {
            Need(args, 1, "TO_INT");
            var src = args[0];
            return r =>
            {
                var s = Str(r, src);
                r[dest] = int.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var i)
                    ? (object)i : 0;
            };
        }

        private static Action<IDictionary<string, object>> Fn_ToFloat(string dest, string[] args)
        {
            Need(args, 1, "TO_FLOAT");
            var src = args[0];
            return r =>
            {
                var s = Str(r, src);
                r[dest] = double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var d)
                    ? (object)d : 0.0;
            };
        }

        private static Action<IDictionary<string, object>> Fn_ToString(string dest, string[] args)
        {
            Need(args, 1, "TO_STRING");
            var src = args[0];
            return r => r[dest] = Str(r, src);
        }

        private static Action<IDictionary<string, object>> Fn_ToBool(string dest, string[] args)
        {
            Need(args, 1, "TO_BOOL");
            var src = args[0];
            return r =>
            {
                var s = Str(r, src).Trim().ToLowerInvariant();
                r[dest] = s is "1" or "true" or "yes" or "on";
            };
        }

        // ── Date / time ───────────────────────────────────────────────────────

        private static Action<IDictionary<string, object>> Fn_Now(string dest)
            => r => r[dest] = DateTime.UtcNow.ToString("o");

        private static Action<IDictionary<string, object>> Fn_Today(string dest)
            => r => r[dest] = DateTime.UtcNow.ToString("yyyy-MM-dd");

        private static Action<IDictionary<string, object>> Fn_FormatDate(string dest, string[] args)
        {
            Need(args, 2, "FORMAT_DATE");
            var src    = args[0];
            var format = args[1];
            return r =>
            {
                var dt = TryParseDate(Str(r, src));
                r[dest] = dt.HasValue ? dt.Value.ToString(format) : Str(r, src);
            };
        }

        private static Action<IDictionary<string, object>> Fn_AddDays(string dest, string[] args)
        {
            Need(args, 2, "ADD_DAYS");
            var src  = args[0];
            var days = double.Parse(args[1], CultureInfo.InvariantCulture);
            return r =>
            {
                var dt = TryParseDate(Str(r, src));
                r[dest] = dt.HasValue ? dt.Value.AddDays(days).ToString("o") : Str(r, src);
            };
        }

        private static Action<IDictionary<string, object>> Fn_DateDiff(string dest, string[] args)
        {
            // DATE_DIFF(field1, field2) → integer days: field2 − field1
            Need(args, 2, "DATE_DIFF");
            var f1 = args[0];
            var f2 = args[1];
            return r =>
            {
                var d1 = TryParseDate(Str(r, f1));
                var d2 = TryParseDate(Str(r, f2));
                r[dest] = (d1.HasValue && d2.HasValue) ? (object)(int)(d2.Value - d1.Value).TotalDays : 0;
            };
        }

        private static Action<IDictionary<string, object>> Fn_Year(string dest, string[] args)
        {
            Need(args, 1, "YEAR");
            var src = args[0];
            return r =>
            {
                var dt = TryParseDate(Str(r, src));
                r[dest] = dt.HasValue ? (object)dt.Value.Year : 0;
            };
        }

        private static Action<IDictionary<string, object>> Fn_Month(string dest, string[] args)
        {
            Need(args, 1, "MONTH");
            var src = args[0];
            return r =>
            {
                var dt = TryParseDate(Str(r, src));
                r[dest] = dt.HasValue ? (object)dt.Value.Month : 0;
            };
        }

        private static Action<IDictionary<string, object>> Fn_Day(string dest, string[] args)
        {
            Need(args, 1, "DAY");
            var src = args[0];
            return r =>
            {
                var dt = TryParseDate(Str(r, src));
                r[dest] = dt.HasValue ? (object)dt.Value.Day : 0;
            };
        }

        // ── Null / conditional ────────────────────────────────────────────────

        private static Action<IDictionary<string, object>> Fn_Coalesce(string dest, string[] args)
        {
            if (args.Length < 2) throw new ArgumentException("COALESCE requires at least 2 arguments.");
            return r =>
            {
                foreach (var f in args)
                {
                    if (r.TryGetValue(f, out var v) && v != null && !string.IsNullOrEmpty(v.ToString()))
                    {
                        r[dest] = v;
                        return;
                    }
                }
                r[dest] = null!;
            };
        }

        private static Action<IDictionary<string, object>> Fn_Default(string dest, string[] args)
        {
            Need(args, 2, "DEFAULT");
            var src    = args[0];
            var defVal = args[1];
            return r =>
            {
                r.TryGetValue(src, out var v);
                r[dest] = (v == null || string.IsNullOrEmpty(v.ToString())) ? (object)defVal : v;
            };
        }

        private static Action<IDictionary<string, object>> Fn_NullIf(string dest, string[] args)
        {
            Need(args, 2, "NULLIF");
            var src      = args[0];
            var nullWhen = args[1];
            return r =>
            {
                var v = Str(r, src);
                r[dest] = v == nullWhen ? null! : (object)v;
            };
        }

        // ── Encoding / hashing ────────────────────────────────────────────────

        private static Action<IDictionary<string, object>> Fn_Md5(string dest, string[] args)
        {
            Need(args, 1, "MD5");
            var src = args[0];
            return r => r[dest] = Convert.ToHexStringLower(
                MD5.HashData(Encoding.UTF8.GetBytes(Str(r, src))));
        }

        private static Action<IDictionary<string, object>> Fn_Sha256(string dest, string[] args)
        {
            Need(args, 1, "SHA256");
            var src = args[0];
            return r => r[dest] = Convert.ToHexStringLower(
                SHA256.HashData(Encoding.UTF8.GetBytes(Str(r, src))));
        }

        private static Action<IDictionary<string, object>> Fn_Base64Encode(string dest, string[] args)
        {
            Need(args, 1, "BASE64_ENCODE");
            var src = args[0];
            return r => r[dest] = Convert.ToBase64String(Encoding.UTF8.GetBytes(Str(r, src)));
        }

        private static Action<IDictionary<string, object>> Fn_Base64Decode(string dest, string[] args)
        {
            Need(args, 1, "BASE64_DECODE");
            var src = args[0];
            return r =>
            {
                try   { r[dest] = Encoding.UTF8.GetString(Convert.FromBase64String(Str(r, src))); }
                catch { r[dest] = string.Empty; }
            };
        }

        private static Action<IDictionary<string, object>> Fn_UrlEncode(string dest, string[] args)
        {
            Need(args, 1, "URL_ENCODE");
            var src = args[0];
            return r => r[dest] = Uri.EscapeDataString(Str(r, src));
        }

        private static Action<IDictionary<string, object>> Fn_UrlDecode(string dest, string[] args)
        {
            Need(args, 1, "URL_DECODE");
            var src = args[0];
            return r => r[dest] = Uri.UnescapeDataString(Str(r, src));
        }
    }
}
