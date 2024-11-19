using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Extractor
{
    internal class Dim_Pais
    {
        public int ID_Pais { get; set; }

        public string Pais { get; set; }

        public string Hemisferio { get; set; }

        public List<Dim_Localidad> Localidades { get; set; } = new List<Dim_Localidad>();
    }
}
