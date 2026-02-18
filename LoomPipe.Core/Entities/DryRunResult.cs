using System.Collections.Generic;

namespace LoomPipe.Core.Entities
{
    public class DryRunResult
    {
        public IEnumerable<dynamic> SourcePreview { get; set; } = Enumerable.Empty<dynamic>();
        public IEnumerable<dynamic> MappedPreview { get; set; } = Enumerable.Empty<dynamic>();
        public IEnumerable<dynamic> TransformedPreview { get; set; } = Enumerable.Empty<dynamic>();
    }
}
