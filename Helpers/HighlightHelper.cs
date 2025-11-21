using Nest;

namespace name_match.Helpers
{
    public static class HighlightHelpers
    {
        public static HighlightDescriptor<Record> ConsolidatedHighlights(HighlightDescriptor<Record> h)
        {
            return h.Fields(f => f
                .Field(f => f.AllNames)
                .Type(HighlighterType.Fvh)
                .RequireFieldMatch(false)
            );
        }
    }
}