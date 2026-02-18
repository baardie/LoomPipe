using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace LoomPipe.Engine
{
    public static class TransformationParser
    {
        // Expression patterns: dest = expr
        private static readonly Regex AssignLiteralRegex    = new(@"^(\w+)\s*=\s*'([^']*)'$", RegexOptions.Compiled);
        private static readonly Regex AssignFieldRegex      = new(@"^(\w+)\s*=\s*(\w+)$", RegexOptions.Compiled);
        private static readonly Regex FieldPlusLiteralRegex = new(@"^(\w+)\s*=\s*(\w+)\s*\+\s*'([^']*)'$", RegexOptions.Compiled);
        private static readonly Regex LiteralPlusFieldRegex = new(@"^(\w+)\s*=\s*'([^']*)'\s*\+\s*(\w+)$", RegexOptions.Compiled);
        private static readonly Regex FieldPlusFieldRegex   = new(@"^(\w+)\s*=\s*(\w+)\s*\+\s*(\w+)$", RegexOptions.Compiled);

        // Function pattern: FUNCTION( args )
        private static readonly Regex FunctionRegex = new(@"^(\w+)\(\s*([^)]+)\s*\)$", RegexOptions.Compiled);

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
                var dest     = m.Groups[1].Value;
                var srcField = m.Groups[2].Value;
                var literal  = m.Groups[3].Value;
                return record =>
                {
                    var val = record.TryGetValue(srcField, out var v) ? v?.ToString() ?? string.Empty : string.Empty;
                    record[dest] = val + literal;
                };
            }

            // dest = 'literal' + field
            m = LiteralPlusFieldRegex.Match(t);
            if (m.Success)
            {
                var dest     = m.Groups[1].Value;
                var literal  = m.Groups[2].Value;
                var srcField = m.Groups[3].Value;
                return record =>
                {
                    var val = record.TryGetValue(srcField, out var v) ? v?.ToString() ?? string.Empty : string.Empty;
                    record[dest] = literal + val;
                };
            }

            // dest = fieldA + fieldB  (numeric add or string concat)
            m = FieldPlusFieldRegex.Match(t);
            if (m.Success)
            {
                var dest   = m.Groups[1].Value;
                var fieldA = m.Groups[2].Value;
                var fieldB = m.Groups[3].Value;
                return record =>
                {
                    record.TryGetValue(fieldA, out var a);
                    record.TryGetValue(fieldB, out var b);
                    record[dest] = AddValues(a, b);
                };
            }

            // dest = srcField  (copy) — must come after field+field to avoid false matches
            m = AssignFieldRegex.Match(t);
            if (m.Success)
            {
                var dest     = m.Groups[1].Value;
                var srcField = m.Groups[2].Value;
                return record =>
                {
                    record[dest] = record.TryGetValue(srcField, out var v) ? v : null!;
                };
            }

            // FUNCTION( args )
            m = FunctionRegex.Match(t);
            if (m.Success)
            {
                var function = m.Groups[1].Value.ToUpperInvariant();
                var args     = m.Groups[2].Value.Split(',');
                for (int i = 0; i < args.Length; i++)
                    args[i] = args[i].Trim().Trim('\'');

                return function switch
                {
                    "UPPER"   => ToUpper(args),
                    "LOWER"   => ToLower(args),
                    "REPLACE" => Replace(args),
                    _         => throw new NotSupportedException($"Function '{function}' is not supported.")
                };
            }

            throw new NotSupportedException($"Transformation '{transformation}' is not supported.");
        }

        private static object AddValues(object? a, object? b)
        {
            if (a is int ia && b is int ib) return ia + ib;
            if (a is double da && b is double db) return da + db;
            if (a is int ia2 && b is double db2) return (double)ia2 + db2;
            if (a is double da2 && b is int ib2) return da2 + ib2;
            return (a?.ToString() ?? string.Empty) + (b?.ToString() ?? string.Empty);
        }

        private static Action<IDictionary<string, object>> ToUpper(string[] args)
        {
            if (args.Length != 1) throw new ArgumentException("UPPER requires 1 argument: field name.");
            var fieldName = args[0];
            return record =>
            {
                if (record.ContainsKey(fieldName) && record[fieldName] is string val)
                    record[fieldName] = val.ToUpperInvariant();
            };
        }

        private static Action<IDictionary<string, object>> ToLower(string[] args)
        {
            if (args.Length != 1) throw new ArgumentException("LOWER requires 1 argument: field name.");
            var fieldName = args[0];
            return record =>
            {
                if (record.ContainsKey(fieldName) && record[fieldName] is string val)
                    record[fieldName] = val.ToLowerInvariant();
            };
        }

        private static Action<IDictionary<string, object>> Replace(string[] args)
        {
            if (args.Length != 3) throw new ArgumentException("REPLACE requires 3 arguments: field name, old value, new value.");
            var (fieldName, oldValue, newValue) = (args[0], args[1], args[2]);
            return record =>
            {
                if (record.ContainsKey(fieldName) && record[fieldName] is string val)
                    record[fieldName] = val.Replace(oldValue, newValue);
            };
        }
    }
}
