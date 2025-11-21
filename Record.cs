using System.Text;

namespace name_match
{

    public class Record
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string FirstName { get; set; }


        public string AllNames
        {
            get
            {
                if (!string.IsNullOrWhiteSpace(FirstName))
                {
                    return $"{Name} {FirstName} | {FirstName} {Name}";
                }
                else
                {
                    return Name;
                }
            }
            private set { }
        }

        public string FurtherInformation { get; set; } // Must contains sample ID card numbers, passport numbers, SSN numbers, etc..
        public string BirthDate { get; set; }
        public string Locations { get; set; }

        // Postal or residential address / domicile
        public string Domicile { get; set; }

    }

}