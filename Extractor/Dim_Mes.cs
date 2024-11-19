using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Extractor
{
    internal class Dim_Mes
    {
        public int ID_Mes { get; set; }

        public int ID_Anio { get; set; }

        public int Mes { get; set; }

        public string Nombre { get; set; }

        public List<Dim_Dia> Dias { get; set; } = new List<Dim_Dia>();
    }
}