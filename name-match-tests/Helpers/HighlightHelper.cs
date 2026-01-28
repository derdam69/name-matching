using Nest;

namespace name_match.Helpers
{
    public static class HighlightHelpers
    {
        public static HighlightDescriptor<Record> ConsolidatedHighlights(HighlightDescriptor<Record> h)
        {
            return h.Fields(f => f
                .Field(ff => ff.AllNames.Suffix("pretoken"))
                .Field(ff => ff.AllNames.Suffix("pretoken_fuzzy"))
                .Type(HighlighterType.Fvh)
                .RequireFieldMatch(false)
            );
        }

        public static bool IsHighlighted(this string input)
        {
            return input.StartsWith("<em>", StringComparison.InvariantCultureIgnoreCase)
            && input.EndsWith("</em>", StringComparison.InvariantCultureIgnoreCase);
        }
    }
}